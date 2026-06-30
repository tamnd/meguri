package store

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// buildArena interns each string the way Store.Intern does (uvarint length then
// bytes, offset 0 reserved as the none sentinel) and returns the flat region plus
// the offset assigned to each input. This is the exact byte layout the resident
// arena and the snapshot string region both use, so a spilledArena reading it must
// match readArena reading it.
func buildArena(strs []string) (arena []byte, offs []uint64) {
	arena = []byte{0} // offset-0 sentinel
	for _, s := range strs {
		if s == "" {
			offs = append(offs, 0)
			continue
		}
		off := uint64(len(arena))
		arena = appendUvarint(arena, uint64(len(s)))
		arena = append(arena, s...)
		offs = append(offs, off)
	}
	return arena, offs
}

// TestSpilledArenaGoldenEquality is the doc 05 section 7b golden gate: a spilled
// read by offset returns the byte-identical string a resident readArena returns
// over the same bytes, for every offset including the none sentinel, the
// out-of-range guard, strings shorter than the over-read window, and strings
// longer than it (the two-read path). Equality here is what lets the redesign
// swap the byte source without changing a single resolved URL.
func TestSpilledArenaGoldenEquality(t *testing.T) {
	strs := []string{
		"https://example.com/",
		"https://example.com/a/b/c?q=1&r=2",
		"x",
		"", // none sentinel -> offset 0
		strings.Repeat("https://long.example.com/path/", 20), // > arenaOverRead, forces the second read
		"https://example.org/" + strings.Repeat("z", 300),    // length prefix is 2 bytes, span past over-read
		"é-unicode-ü-path/かな",
	}
	arena, offs := buildArena(strs)

	// Budget large enough to hold everything, then a second pass with a tiny
	// budget so most reads are cold preads. Both must still match readArena.
	for _, budget := range []int64{1 << 20, 0, 64} {
		sa := newSpilledArena(bytes.NewReader(arena), int64(len(arena)), budget)
		for i, off := range offs {
			want := readArena(arena, off)
			got := sa.readArenaAt(off)
			if got != want {
				t.Fatalf("budget=%d str[%d] off=%d: spilled=%q resident=%q", budget, i, off, got, want)
			}
		}
		// Out-of-range and corrupt-length offsets degrade to empty, like readArena.
		for _, off := range []uint64{0, uint64(len(arena)), uint64(len(arena)) + 99} {
			if got := sa.readArenaAt(off); got != "" {
				t.Fatalf("budget=%d off=%d: want empty, got %q", budget, off, got)
			}
		}
	}
}

// TestSpilledArenaRepeatHits checks a hot offset is served from the cache on the
// second read (the working-set locality doc 05 section 3d relies on) and stays
// byte-identical. It reads each offset twice and asserts the hit counter rises.
func TestSpilledArenaRepeatHits(t *testing.T) {
	strs := make([]string, 200)
	for i := range strs {
		strs[i] = fmt.Sprintf("https://host%03d.example.com/page/%d", i, i)
	}
	arena, offs := buildArena(strs)
	sa := newSpilledArena(bytes.NewReader(arena), int64(len(arena)), 1<<20)

	for _, off := range offs {
		_ = sa.readArenaAt(off)
	}
	for _, off := range offs {
		if got, want := sa.readArenaAt(off), readArena(arena, off); got != want {
			t.Fatalf("off=%d: %q != %q", off, got, want)
		}
	}
	_, _, hits, _, _ := sa.stats()
	if hits == 0 {
		t.Fatalf("expected cache hits on the second pass, got 0")
	}
}

// TestSpilledArenaBoundedResidency is the residency invariant: serving every
// offset in an arena far larger than the budget keeps resident bytes at or under
// B_arena (doc 05 section 3, doc 03 the B_arena term). The cache must never grow
// to O(N); that flat bound is the whole point of the spill.
func TestSpilledArenaBoundedResidency(t *testing.T) {
	const n = 5000
	strs := make([]string, n)
	for i := range strs {
		strs[i] = fmt.Sprintf("https://host%05d.example.com/some/canonical/path?id=%d", i, i)
	}
	arena, offs := buildArena(strs)
	const budget = 64 << 10 // 64 KiB, far below the full arena
	sa := newSpilledArena(bytes.NewReader(arena), int64(len(arena)), budget)

	for _, off := range offs {
		if got, want := sa.readArenaAt(off), readArena(arena, off); got != want {
			t.Fatalf("off=%d: %q != %q", off, got, want)
		}
		used, b, _, _, _ := sa.stats()
		if used > b {
			t.Fatalf("resident bytes %d exceeded budget %d", used, b)
		}
	}
	used, _, _, _, evicted := sa.stats()
	if evicted == 0 {
		t.Fatalf("expected evictions serving %d urls under a %d-byte budget, got 0", n, budget)
	}
	if int64(used) > budget {
		t.Fatalf("final resident %d over budget %d", used, budget)
	}
}
