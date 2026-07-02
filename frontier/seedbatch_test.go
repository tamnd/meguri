package frontier

import (
	"bytes"
	"testing"
)

// specsFrom converts the test's seed records into the SeedSpec the batch intake
// takes, the same fields the Seed loop passes positionally.
func specsFrom(seeds []seed) []SeedSpec {
	out := make([]SeedSpec, len(seeds))
	for i, s := range seeds {
		out[i] = SeedSpec{URL: s.url, Host: s.host, Priority: s.priority, CrawlDelay: s.delay}
	}
	return out
}

// TestSeedBatchMatchesSeedLoop is the equivalence proof the optimization rests on:
// a frontier seeded by SeedBatch must be byte-for-byte the one seeded by the
// equivalent Seed loop, because SeedBatch only moves the dedup insert off the hot
// path, it changes nothing about which records exist or how they schedule. The
// test compares both the checkpoint bytes and the full dispatch sequence.
func TestSeedBatchMatchesSeedLoop(t *testing.T) {
	seeds := syntheticSeeds(12, 6, 20)

	loop := seedAll(New(1, 0), seeds)
	batch := New(1, 0)
	batch.SeedBatch(specsFrom(seeds))

	if loop.Len() != batch.Len() {
		t.Fatalf("Len mismatch: loop %d, batch %d", loop.Len(), batch.Len())
	}

	loopRaw, err := loop.CheckpointBytes()
	if err != nil {
		t.Fatalf("loop checkpoint: %v", err)
	}
	batchRaw, err := batch.CheckpointBytes()
	if err != nil {
		t.Fatalf("batch checkpoint: %v", err)
	}
	if !bytes.Equal(loopRaw, batchRaw) {
		t.Fatalf("checkpoint bytes diverge: loop %d bytes, batch %d bytes", len(loopRaw), len(batchRaw))
	}

	refSeq := drain(t, seedAll(New(1, 0), seeds), 0)
	gotSeq := drain(t, func() *Frontier { f := New(1, 0); f.SeedBatch(specsFrom(seeds)); return f }(), 0)
	if len(gotSeq) != len(refSeq) {
		t.Fatalf("dispatch length: batch %d, loop %d", len(gotSeq), len(refSeq))
	}
	for i := range refSeq {
		if gotSeq[i].Key != refSeq[i].Key {
			t.Fatalf("dispatch %d diverged: batch %x, loop %x", i, gotSeq[i].Key.Bytes(), refSeq[i].Key.Bytes())
		}
	}
}

// TestSeedBatchDeduplicates checks SeedBatch is idempotent exactly as Seed is: a
// key repeated within the window and a key already resident from a prior batch
// both fold to one record, never two. This is the seen-set authority doing its job
// through the batch path instead of the per-key one.
func TestSeedBatchDeduplicates(t *testing.T) {
	f := New(1, 0)
	specs := []SeedSpec{
		{URL: "http://a.test/x", Host: "a.test", Priority: 0.5, CrawlDelay: 10},
		{URL: "http://a.test/x", Host: "a.test", Priority: 0.5, CrawlDelay: 10}, // repeat in window
		{URL: "http://a.test/y", Host: "a.test", Priority: 0.5, CrawlDelay: 10},
		{URL: "http://b.test/z", Host: "b.test", Priority: 0.5, CrawlDelay: 10},
	}
	f.SeedBatch(specs)
	if f.Len() != 3 {
		t.Fatalf("within-window dedup: Len %d, want 3", f.Len())
	}

	// A second batch repeating resident keys plus one new key adds only the new one.
	f.SeedBatch([]SeedSpec{
		{URL: "http://a.test/x", Host: "a.test", Priority: 0.5, CrawlDelay: 10}, // resident
		{URL: "http://b.test/z", Host: "b.test", Priority: 0.5, CrawlDelay: 10}, // resident
		{URL: "http://c.test/w", Host: "c.test", Priority: 0.5, CrawlDelay: 10}, // new
	})
	if f.Len() != 4 {
		t.Fatalf("cross-batch dedup: Len %d, want 4", f.Len())
	}
}

// TestSeedBatchEmpty is the trivial guard: an empty window is a no-op, not a panic.
func TestSeedBatchEmpty(t *testing.T) {
	f := New(1, 0)
	f.SeedBatch(nil)
	f.SeedBatch([]SeedSpec{})
	if f.Len() != 0 {
		t.Fatalf("empty SeedBatch left Len %d, want 0", f.Len())
	}
}
