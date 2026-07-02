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

// countInternFrames scans the store's current log and returns how many kindIntern
// frames it holds, the per-string byte tax #43 set out to remove from the durable
// tail when arena.bin is the string region's home.
func countInternFrames(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.log.replay(func(f frame) error {
		if f.kind == kindIntern {
			n++
		}
		return nil
	}); err != nil {
		t.Fatalf("replay log: %v", err)
	}
	return n
}

// TestDiskIndexSpillNoInternFrames is the #43 invariant: when the index is the DRUM
// and the arena is spilled, the string region lives once in arena.bin, so the log
// must carry no kindIntern frames at all. The same ingest into a disk-index store
// without the spill keeps logging the frames, so the two counts bracket the change:
// zero with the spill, one per distinct interned string without it.
func TestDiskIndexSpillNoInternFrames(t *testing.T) {
	const n = 2000
	ingest := func(opt Options) *Store {
		s, err := Open(t.TempDir(), opt)
		if err != nil {
			t.Fatal(err)
		}
		for i := range n {
			s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%20), fmt.Sprintf("/p%d", i), float32(i)))
		}
		return s
	}

	spill := ingest(Options{Durability: DurabilityNormal, DiskIndex: true, SpillArena: true, ArenaBudget: 16 << 10})
	defer spill.Close()
	if got := countInternFrames(t, spill); got != 0 {
		t.Fatalf("disk-index spill log carried %d kindIntern frames, want 0 (the strings are in arena.bin)", got)
	}

	plain := ingest(Options{Durability: DurabilityNormal, DiskIndex: true})
	defer plain.Close()
	// Each of the n distinct URL strings is interned and logged once, so the plain
	// log carries one frame per URL: the per-string tax the spill removes entirely.
	if got := countInternFrames(t, plain); got < n {
		t.Fatalf("disk-index plain log carried %d kindIntern frames, want >= %d", got, n)
	}
}

// TestDiskIndexSpillBoundedRecovery guards the latent OOM the #43 reopen path
// closes: recovery must not rebuild the whole string region into RAM. After a
// reopen the resident arena is nil and arena.bin is the source read through the
// bounded LRU, so a 100M arena never has to fit in memory to recover. The pre-#43
// path replayed every kindIntern frame back into a resident s.arena, which is the
// 7.87 GB transient doc 01 named.
func TestDiskIndexSpillBoundedRecovery(t *testing.T) {
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
	if r.arena != nil {
		t.Fatalf("recovery rebuilt a resident arena of %d bytes, want nil (spill file is the source)", len(r.arena))
	}
	if r.spill == nil {
		t.Fatal("reopened disk-index spill store has no spill reader")
	}
	// The strings still resolve, read back through the bounded reader off arena.bin.
	got, ok := r.GetURL(meguri.MakeURLKey("h5.test", "/p25"))
	if !ok || r.Str(got.URLRef) != "http://h5.test/p25" {
		t.Fatalf("string unreadable after bounded recovery: ok=%v url=%q", ok, r.Str(got.URLRef))
	}
}

// TestDiskIndexSpillPostCheckpointSurvives covers the Close flush path: a string
// interned after the checkpoint lives only in the arena.bin tail, not in any
// snapshot, so Close must flush and sync the pending tail or the reopened URLRef
// dangles. With the kindIntern frame dropped there is no log copy to fall back on,
// which is exactly why Close grew the spill flush.
func TestDiskIndexSpillPostCheckpointSurvives(t *testing.T) {
	dir := t.TempDir()
	opt := Options{Durability: DurabilityNormal, DiskIndex: true, SpillArena: true, ArenaBudget: 16 << 10}
	s, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%20), fmt.Sprintf("/p%d", i), float32(i)))
	}
	if err := s.CheckpointStreaming(256); err != nil {
		t.Fatal(err)
	}
	// This URL is interned only after the checkpoint, so it lives in the arena tail
	// alone and Close must persist it.
	post := mkURL(t, s, "late.test", "/fresh", 7)
	if _, err := s.PutURL(post); err != nil {
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
	got, ok := r.GetURL(meguri.MakeURLKey("late.test", "/fresh"))
	if !ok || r.Str(got.URLRef) != "http://late.test/fresh" {
		t.Fatalf("post-checkpoint intern lost across reopen: ok=%v url=%q", ok, r.Str(got.URLRef))
	}
}
