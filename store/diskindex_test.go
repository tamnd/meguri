package store

import (
	"fmt"
	"testing"

	"github.com/tamnd/meguri"
)

// TestDiskIndexRoundTrip is the Stage B point path (spec 2072 doc 04): with the URL
// index of record on disk in the DRUM, a put then a get returns the same record,
// the resident shard maps stay empty (no per-url term), and a re-put supersedes by
// LSN. This is the disk-index counterpart to TestPutGetRoundTrip.
func TestDiskIndexRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir(), Options{Durability: DurabilityFull, DiskIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rec := mkURL(t, s, "go.dev", "/doc/", 0.5)
	if _, err := s.PutURL(rec); err != nil {
		t.Fatal(err)
	}
	got, ok := s.GetURL(rec.URLKey)
	if !ok || got.Priority != 0.5 {
		t.Fatalf("get after put: ok=%v priority=%v", ok, got.Priority)
	}
	if s.Str(got.URLRef) != "http://go.dev/doc/" {
		t.Fatalf("arena string wrong: %q", s.Str(got.URLRef))
	}

	// The resident shard maps must hold no URLs in disk-index mode.
	var resident int
	for i := range s.shards {
		resident += len(s.shards[i].m)
	}
	if resident != 0 {
		t.Fatalf("disk-index mode kept %d resident URL slots, want 0", resident)
	}

	// A re-put with a higher priority supersedes by LSN.
	rec2 := mkURL(t, s, "go.dev", "/doc/", 0.9)
	rec2.CrawlCount = 3
	if _, err := s.PutURL(rec2); err != nil {
		t.Fatal(err)
	}
	got, ok = s.GetURL(rec.URLKey)
	if !ok || got.Priority != 0.9 || got.CrawlCount != 3 {
		t.Fatalf("re-put not superseded: ok=%v %+v", ok, got)
	}
}

// TestDiskIndexCheckpointRecover is the Stage B durability gate: ingest into the
// disk index, checkpoint (which folds the DRUM into the repository and streams the
// snapshot from it), update past the checkpoint, then reopen. The repository is the
// durable index of record, so the snapshot rows come back from the reopened
// repository and the post-checkpoint update replays on top by LSN.
func TestDiskIndexCheckpointRecover(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityNormal, DiskIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	const n = 4000
	for i := range n {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%20), fmt.Sprintf("/p%d", i), float32(i)))
	}
	for i := range 20 {
		s.PutHost(&meguri.HostRecord{HostKey: meguri.HostKeyOf(fmt.Sprintf("h%d.test", i)), URLBudget: uint32(64 + i)})
	}
	if err := s.CheckpointStreaming(256); err != nil {
		t.Fatal(err)
	}
	// A post-checkpoint update lands in the fresh log and must replay on top.
	post := mkURL(t, s, "h3.test", "/p3", 99)
	post.CrawlCount = 7
	s.PutURL(post)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(dir, Options{Durability: DurabilityNormal, DiskIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.URLCount() != n {
		t.Fatalf("recovered %d URLs, want %d", r.URLCount(), n)
	}
	if r.HostCount() != 20 {
		t.Fatalf("recovered %d hosts, want 20", r.HostCount())
	}
	got, ok := r.GetURL(meguri.MakeURLKey("h3.test", "/p3"))
	if !ok || got.Priority != 99 || got.CrawlCount != 7 {
		t.Fatalf("post-checkpoint update not replayed on top: ok=%v %+v", ok, got)
	}
	if r.Str(got.URLRef) != "http://h3.test/p3" {
		t.Fatalf("arena not recovered across the streamed checkpoint: %q", r.Str(got.URLRef))
	}
	// A record that was only ever in the snapshot, never re-logged, must survive.
	snapOnly, ok := r.GetURL(meguri.MakeURLKey("h7.test", "/p7"))
	if !ok || r.Str(snapOnly.URLRef) != "http://h7.test/p7" {
		t.Fatalf("snapshot-only record lost: ok=%v url=%q", ok, r.Str(snapOnly.URLRef))
	}
}

// TestDiskIndexSpillArena is the full 100M run config: the URL index on disk (DRUM)
// AND the string arena spilled to disk, the two B/url terms the size ladder named.
// Both must survive a streamed checkpoint and a reopen.
func TestDiskIndexSpillArena(t *testing.T) {
	dir := t.TempDir()
	opt := Options{Durability: DurabilityNormal, DiskIndex: true, SpillArena: true, ArenaBudget: 16 << 10}
	s, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	const n = 4000
	for i := range n {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%20), fmt.Sprintf("/p%d", i), float32(i)))
	}
	if err := s.CheckpointStreaming(256); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.URLCount() != n {
		t.Fatalf("recovered %d URLs, want %d", r.URLCount(), n)
	}
	got, ok := r.GetURL(meguri.MakeURLKey("h3.test", "/p3"))
	if !ok {
		t.Fatal("record h3.test/p3 missing after disk-index spill checkpoint")
	}
	if u := r.Str(got.URLRef); u != "http://h3.test/p3" {
		t.Fatalf("arena string lost across disk-index spill checkpoint: got %q want %q", u, "http://h3.test/p3")
	}
}
