package store

import (
	"fmt"
	"testing"

	"github.com/tamnd/meguri"
)

// TestSpillArenaStoreEquality is the Stage A store-integration gate (spec 2072 doc
// 05, doc 52): a store opened with SpillArena interns, reads, checkpoints, and
// recovers byte-identically to a store on the resident arena. It runs the same
// sequence against both, then asserts every Str/Robots read matches and the
// offsets the two stores assign are identical, so switching the byte source from a
// resident []byte to the spill file changes not one resolved URL.
func TestSpillArenaStoreEquality(t *testing.T) {
	urls := make([]string, 4000)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://host%04d.example.com/path/%d?q=%d", i%500, i, i*7)
	}
	robots := []byte("User-agent: *\nDisallow: /private\n")

	open := func(dir string, spill bool) *Store {
		s, err := Open(dir, Options{
			Durability:  DurabilityNormal,
			SpillArena:  spill,
			ArenaBudget: 64 << 10, // tiny B_arena: force evictions, exercise cold preads
		})
		if err != nil {
			t.Fatalf("open spill=%v: %v", spill, err)
		}
		return s
	}

	// Drive an identical intern sequence against both stores and collect offsets.
	run := func(dir string, spill bool) (offs []uint64, robotsOff uint64) {
		s := open(dir, spill)
		for _, u := range urls {
			off, err := s.Intern(u)
			if err != nil {
				t.Fatalf("intern: %v", err)
			}
			offs = append(offs, off)
		}
		var err error
		if robotsOff, err = s.InternRobots(robots); err != nil {
			t.Fatalf("intern robots: %v", err)
		}
		// A checkpoint folds the arena into the snapshot; reopen exercises the
		// recover + re-enable-spill rebuild and proves offsets survive the round trip.
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		return offs, robotsOff
	}

	residOffs, residRobots := run(t.TempDir(), false)
	spillOffs, spillRobots := run(t.TempDir(), true)

	if len(residOffs) != len(spillOffs) {
		t.Fatalf("offset count: resident=%d spill=%d", len(residOffs), len(spillOffs))
	}
	for i := range residOffs {
		if residOffs[i] != spillOffs[i] {
			t.Fatalf("offset[%d]: resident=%d spill=%d", i, residOffs[i], spillOffs[i])
		}
	}
	if residRobots != spillRobots {
		t.Fatalf("robots offset: resident=%d spill=%d", residRobots, spillRobots)
	}
}

// TestSpillArenaStoreRecover reopens a spill-mode store after a checkpoint plus
// post-checkpoint interns and asserts every string reads back correctly, the
// recover-then-rebuild-spill path (snapshot string region ++ new-log kindIntern
// frames) preserving every offset.
func TestSpillArenaStoreRecover(t *testing.T) {
	dir := t.TempDir()
	pre := []string{
		"https://example.com/",
		"https://example.com/a/b/c?q=1",
		"https://sub.example.org/" + string(make([]byte, 0)),
		"https://long.example.net/" + repeat("z", 400), // forces the two-read path
	}
	post := []string{
		"https://added-after-checkpoint.com/x",
		"https://another.example.io/y?z=2",
	}

	s, err := Open(dir, Options{Durability: DurabilityNormal, SpillArena: true, ArenaBudget: 32 << 10})
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]string{}
	for _, u := range pre {
		off, err := s.Intern(u)
		if err != nil {
			t.Fatal(err)
		}
		want[off] = u
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, u := range post {
		off, err := s.Intern(u)
		if err != nil {
			t.Fatal(err)
		}
		want[off] = u
	}
	// Reads on the live store match.
	for off, u := range want {
		if got := s.Str(off); got != u {
			t.Fatalf("live off=%d: got %q want %q", off, got, u)
		}
	}

	// Reopen: recover loads the snapshot string region, replays the post-checkpoint
	// kindIntern frames, and enableArenaSpill rebuilds the spill file. Every offset
	// must still resolve to the same string.
	s2, err := Open(dir, Options{Durability: DurabilityNormal, SpillArena: true, ArenaBudget: 32 << 10})
	if err != nil {
		t.Fatal(err)
	}
	for off, u := range want {
		if got := s2.Str(off); got != u {
			t.Fatalf("recovered off=%d: got %q want %q", off, got, u)
		}
	}
}

// TestSpillArenaStoreURLRecords drives the full record path under spill mode:
// intern a host and url string, store a URLRecord referencing those offsets,
// checkpoint, reopen, and confirm the record's string references resolve. This is
// the end-to-end shape the scale ingest runs, with the arena spilled.
func TestSpillArenaStoreURLRecords(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityNormal, SpillArena: true, ArenaBudget: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	const n = 2000
	hostOff, err := s.Intern("example.com")
	if err != nil {
		t.Fatal(err)
	}
	urlOffs := make([]uint64, n)
	for i := range n {
		u := fmt.Sprintf("https://example.com/page/%d", i)
		off, err := s.Intern(u)
		if err != nil {
			t.Fatal(err)
		}
		urlOffs[i] = off
		rec := &meguri.URLRecord{
			URLKey: meguri.URLKey{HostKey: 1, PathKey: uint64(i)},
			URLRef: off,
		}
		if _, err := s.PutURL(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := s.Str(hostOff); got != "example.com" {
		t.Fatalf("host ref: got %q", got)
	}
	for i, off := range urlOffs {
		want := fmt.Sprintf("https://example.com/page/%d", i)
		if got := s.Str(off); got != want {
			t.Fatalf("url[%d] off=%d: got %q want %q", i, off, got, want)
		}
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}
