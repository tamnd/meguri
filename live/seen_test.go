package live

import (
	"bytes"
	"slices"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// spillMix is a splitmix64 step, used to fabricate distinct keys for the seal tests.
func spillMix(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// sortedSealKeys builds n distinct URLKeys in ascending URLKey order, the order every
// seal path hands keys to the collector.
func sortedSealKeys(n int) []m.URLKey {
	keys := make([]m.URLKey, n)
	for i := range keys {
		keys[i] = m.URLKey{
			HostKey: spillMix(uint64(i) * 0x100000001B3),
			PathKey: spillMix(uint64(i)*0xC2B2AE3D27D4EB4F + 1),
		}
	}
	slices.SortFunc(keys, func(a, b m.URLKey) int { return a.Compare(b) })
	return keys
}

// TestShardSpillMatchesInMemory is the equivalence gate for the disk-backed seal: a
// shardSpill fed the sorted key stream must produce the exact same filter blob the
// in-memory sharded builder produces over the same partition. Byte equality proves the
// spill changed only where the keys live during the solve, not the frozen filter, so a
// 100M seal is lossless against the in-memory form.
func TestShardSpillMatchesInMemory(t *testing.T) {
	const shardCount = 8
	keys := sortedSealKeys(20000)
	r := dedup.RibbonBitsForFPR(0.01)

	sp, err := newShardSpill(t.TempDir(), shardCount)
	if err != nil {
		t.Fatalf("newShardSpill: %v", err)
	}
	for _, k := range keys {
		if err := sp.add(k); err != nil {
			t.Fatalf("spill add: %v", err)
		}
	}
	diskBlob, err := sp.build(r)
	if err != nil {
		t.Fatalf("spill build: %v", err)
	}

	shards := make([][]m.URLKey, shardCount)
	for _, k := range keys {
		i := dedup.RibbonShardIndex(k, shardCount)
		shards[i] = append(shards[i], k)
	}
	memBlob, err := dedup.BuildShardedRibbonFilter(shards, dedup.WithRibbonBits(r))
	if err != nil {
		t.Fatalf("in-memory build: %v", err)
	}

	if !bytes.Equal(diskBlob, memBlob) {
		t.Fatalf("disk-spill blob differs from in-memory blob (%d vs %d bytes)", len(diskBlob), len(memBlob))
	}

	rf, err := dedup.UnmarshalFilter(diskBlob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(len(keys)) {
		t.Fatalf("filter holds %d keys, want %d", rf.Len(), len(keys))
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("key %d false-negative through the spilled seal", i)
		}
	}
}

// TestSeenBuilderSpillsAtScale checks newSeenBuilder switches to the disk spill once the
// key count crosses the shard threshold, and that the end-to-end collector (auto shard
// decision, per-shard routing, tail dedup, solve) freezes a filter that holds every key
// with no false negative. It feeds a duplicate of each key to exercise the tail drop.
func TestSeenBuilderSpillsAtScale(t *testing.T) {
	// Past the single-shard threshold so RibbonShardCount picks more than one shard and
	// the collector spills.
	keys := sortedSealKeys(300_000)

	s, err := newSeenBuilder(0.01, uint64(len(keys)), t.TempDir())
	if err != nil {
		t.Fatalf("newSeenBuilder: %v", err)
	}
	if s.spill == nil {
		t.Skip("open-file budget too tight for the spill on this platform; in-memory path covered elsewhere")
	}
	for _, k := range keys {
		if err := s.addSorted(k); err != nil {
			t.Fatalf("addSorted: %v", err)
		}
		if err := s.addSorted(k); err != nil { // adjacent duplicate, must be dropped
			t.Fatalf("addSorted dup: %v", err)
		}
	}
	blob, bits, err := s.marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bits <= 0 {
		t.Fatalf("bits per url = %.2f, want positive", bits)
	}
	rf, err := dedup.UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(len(keys)) {
		t.Fatalf("filter holds %d keys, want %d distinct (dups must drop)", rf.Len(), len(keys))
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("key %d false-negative through the scaled spill seal", i)
		}
	}
}
