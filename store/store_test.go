package store

import (
	"fmt"
	"testing"

	"github.com/tamnd/meguri"
)

// mkURL builds a URL record for host/path with the given priority and a canonical
// URL interned into the store, returning the record ready to Put.
func mkURL(t *testing.T, s *Store, host, path string, priority float32) *meguri.URLRecord {
	t.Helper()
	url := "http://" + host + path
	ref, err := s.Intern(url)
	if err != nil {
		t.Fatalf("intern: %v", err)
	}
	return &meguri.URLRecord{
		URLKey:          meguri.MakeURLKey(host, path),
		HostKey:         meguri.HostKeyOf(host),
		Status:          meguri.StatusScheduled,
		Priority:        priority,
		URLRef:          ref,
		FirstSeen:       100,
		DiscoverySource: meguri.SourceSeed,
	}
}

// TestPutGetRoundTrip is the point-update path: a put then a get returns the same
// record, and a second put with new fields supersedes the first by LSN.
func TestPutGetRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir(), Options{Durability: DurabilityFull})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rec := mkURL(t, s, "go.dev", "/doc/", 0.5)
	if _, err := s.PutURL(rec); err != nil {
		t.Fatal(err)
	}
	got, ok := s.GetURL(rec.URLKey)
	if !ok {
		t.Fatal("get missed a put")
	}
	if got.Priority != 0.5 || s.Str(got.URLRef) != "http://go.dev/doc/" {
		t.Fatalf("round-trip mismatch: pri=%v url=%q", got.Priority, s.Str(got.URLRef))
	}

	rec.Priority = 0.9
	rec.CrawlCount = 3
	if _, err := s.PutURL(rec); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetURL(rec.URLKey)
	if got.Priority != 0.9 || got.CrawlCount != 3 {
		t.Fatalf("update did not supersede: %+v", got)
	}
}

// TestHostRoundTrip checks the host store: a put then a get returns the record.
func TestHostRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	h := &meguri.HostRecord{HostKey: meguri.HostKeyOf("go.dev"), URLBudget: 96, CrawlDelay: 15}
	if _, err := s.PutHost(h); err != nil {
		t.Fatal(err)
	}
	got, ok := s.GetHost(h.HostKey)
	if !ok || got.URLBudget != 96 || got.CrawlDelay != 15 {
		t.Fatalf("host round-trip: ok=%v %+v", ok, got)
	}
}

// TestTombstoneHides checks a deleted key reads back as absent.
func TestTombstoneHides(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rec := mkURL(t, s, "go.dev", "/x", 0.1)
	s.PutURL(rec)
	if _, ok := s.GetURL(rec.URLKey); !ok {
		t.Fatal("missing before delete")
	}
	if err := s.DeleteURL(rec.URLKey); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetURL(rec.URLKey); ok {
		t.Fatal("tombstoned key still visible")
	}
}

// TestReopenReplaysLog is the crash-recovery contract without a checkpoint: a
// store that closes (or crashes) before its first checkpoint recovers every
// durable record by replaying the log, the log-as-journal property (doc 11
// section 5). Reopening rebuilds the index, the string arena, and the LSN
// counter from the log alone.
func TestReopenReplaysLog(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityFull})
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	for i := range n {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%20), fmt.Sprintf("/p%d", i), float32(i)/n))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(dir, Options{Durability: DurabilityFull})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := r.URLCount(); got != n {
		t.Fatalf("recovered %d URLs, want %d", got, n)
	}
	// Spot-check a record and its interned URL survived the replay.
	key := meguri.MakeURLKey("h7.test", "/p7")
	rec, ok := r.GetURL(key)
	if !ok || r.Str(rec.URLRef) != "http://h7.test/p7" {
		t.Fatalf("replay lost a record: ok=%v url=%q", ok, r.Str(rec.URLRef))
	}
}

// TestCheckpointRecoverIdentity is the checkpoint round-trip: every live record
// folds into the .meguri snapshot, the log rotates, and a recovery from the
// snapshot plus the (empty) tail rebuilds an identical store (doc 11 section 4,
// 5). Records updated after the checkpoint land in the new log and replay on top.
func TestCheckpointRecoverIdentity(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityNormal})
	if err != nil {
		t.Fatal(err)
	}
	const n = 300
	for i := range n {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%10), fmt.Sprintf("/p%d", i), float32(i)))
	}
	for i := range 10 {
		s.PutHost(&meguri.HostRecord{HostKey: meguri.HostKeyOf(fmt.Sprintf("h%d.test", i)), URLBudget: uint32(64 + i)})
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// A post-checkpoint update goes to the fresh log.
	post := mkURL(t, s, "h3.test", "/p3", 99)
	post.CrawlCount = 7
	s.PutURL(post)
	s.Close()

	r, err := Open(dir, Options{Durability: DurabilityNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.URLCount() != n {
		t.Fatalf("recovered %d URLs, want %d", r.URLCount(), n)
	}
	if r.HostCount() != 10 {
		t.Fatalf("recovered %d hosts, want 10", r.HostCount())
	}
	got, ok := r.GetURL(meguri.MakeURLKey("h3.test", "/p3"))
	if !ok || got.Priority != 99 || got.CrawlCount != 7 {
		t.Fatalf("post-checkpoint update not replayed on top of the snapshot: %+v", got)
	}
	if r.Str(got.URLRef) != "http://h3.test/p3" {
		t.Fatalf("arena not recovered across checkpoint: %q", r.Str(got.URLRef))
	}
}

// TestLargerThanMemorySpill checks the hybrid-log behavior: with a small resident
// budget the store evicts cold record bodies to disk and re-materializes them
// from the log on read, so the resident set stays bounded while every record
// stays reachable (doc 11 section 6).
func TestLargerThanMemorySpill(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityNone, ResidentBudget: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 2000
	for i := range n {
		s.PutURL(mkURL(t, s, "host.test", fmt.Sprintf("/p%d", i), float32(i)))
	}
	if res := s.Resident(); res > 64 {
		t.Fatalf("resident set %d exceeded the budget of 64", res)
	}
	if s.URLCount() != n {
		t.Fatalf("URL count %d, want %d (eviction must not lose keys)", s.URLCount(), n)
	}
	// Every evicted record re-materializes correctly from the log.
	for _, i := range []int{0, 1, 500, 1234, n - 1} {
		key := meguri.MakeURLKey("host.test", fmt.Sprintf("/p%d", i))
		rec, ok := s.GetURL(key)
		if !ok || rec.Priority != float32(i) {
			t.Fatalf("spilled read of p%d wrong: ok=%v pri=%v", i, ok, rec.Priority)
		}
	}
}

// TestRecoverSpilledRecord checks the spilled-read path survives a reopen: write
// past the budget, close, reopen, and a cold record still re-materializes from
// the replayed log.
func TestRecoverSpilledRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityFull, ResidentBudget: 32})
	if err != nil {
		t.Fatal(err)
	}
	const n = 400
	for i := range n {
		s.PutURL(mkURL(t, s, "host.test", fmt.Sprintf("/p%d", i), float32(i)))
	}
	s.Close()

	r, err := Open(dir, Options{Durability: DurabilityFull, ResidentBudget: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.URLCount() != n {
		t.Fatalf("recovered %d, want %d", r.URLCount(), n)
	}
	rec, ok := r.GetURL(meguri.MakeURLKey("host.test", "/p123"))
	if !ok || rec.Priority != 123 {
		t.Fatalf("cold record lost across reopen: ok=%v pri=%v", ok, rec.Priority)
	}
}

// TestTwoSlotSuperblockSurvivesGenerations checks consecutive checkpoints
// alternate slots and recovery always picks the latest valid one.
func TestTwoSlotSuperblockSurvivesGenerations(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for gen := range 5 {
		s.PutURL(mkURL(t, s, "go.dev", fmt.Sprintf("/g%d", gen), float32(gen)))
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("checkpoint %d: %v", gen, err)
		}
	}
	s.Close()

	r, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.URLCount() != 5 {
		t.Fatalf("after 5 checkpoints recovered %d URLs, want 5", r.URLCount())
	}
}
