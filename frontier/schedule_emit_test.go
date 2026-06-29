package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// TestDurableScheduleEmitMatchesColumn gates decision D13: a wheel-on checkpoint
// serializes the durable timing-wheel region, and reading due work through the
// wheel (DueByWheel) returns exactly the set a full next_due column scan (DueKeys)
// returns. The wheel is a pruning accelerator, never a different answer. A wheel-off
// checkpoint carries no region and the wheel read falls back to the column scan, so
// the default file stays byte-for-byte unchanged.
func TestDurableScheduleEmitMatchesColumn(t *testing.T) {
	build := func(wheel bool) []byte {
		var opts []Option
		opts = append(opts, WithStateMachine())
		if wheel {
			opts = append(opts, WithScheduleIndex())
		}
		f := New(1, 0, opts...)
		// Three URLs due by hour 10, three dated out to hour 5000, across two hosts.
		for i, h := range []string{"a.example", "b.example"} {
			for j := range 3 {
				due := uint32(10)
				if i == 1 {
					due = 5000 // host b is all future
				}
				_ = j
				f.Seed("https://"+h+"/p/"+string(rune('a'+j)), h, 0.5, 0, due, 10)
			}
		}
		blob, err := f.CheckpointBytes()
		if err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		return blob
	}

	const before = uint32(100)

	withWheel := build(true)
	rW, err := format.NewReader(withWheel)
	if err != nil {
		t.Fatalf("reader (wheel): %v", err)
	}
	if !rW.HasSchedule() {
		t.Fatal("wheel-on checkpoint did not carry the durable schedule region")
	}
	wheelKeys, err := rW.DueByWheel(before)
	if err != nil {
		t.Fatalf("DueByWheel: %v", err)
	}
	colKeys, err := rW.DueKeys(before)
	if err != nil {
		t.Fatalf("DueKeys: %v", err)
	}
	if !sameKeySet(wheelKeys, colKeys) {
		t.Fatalf("wheel pushdown %d keys != column scan %d keys", len(wheelKeys), len(colKeys))
	}
	if len(wheelKeys) != 3 {
		t.Fatalf("due by hour %d = %d keys, want 3 (host a only)", before, len(wheelKeys))
	}

	noWheel := build(false)
	rN, err := format.NewReader(noWheel)
	if err != nil {
		t.Fatalf("reader (no wheel): %v", err)
	}
	if rN.HasSchedule() {
		t.Fatal("wheel-off checkpoint carried a schedule region, the default file must stay unchanged")
	}
	fallback, err := rN.DueByWheel(before) // no region: falls back to the column scan
	if err != nil {
		t.Fatalf("DueByWheel fallback: %v", err)
	}
	if !sameKeySet(fallback, colKeys) {
		t.Fatalf("wheel-off fallback %d keys != column scan %d keys", len(fallback), len(colKeys))
	}
}

// TestDueURLsLive gates the live schedule read: the URLs due at or before a horizon
// come back sorted by due time with their canonical strings, filtered by host and
// capped by the limit. Two URLs are due soon, two dated far out.
func TestDueURLsLive(t *testing.T) {
	f := New(1, 0, WithScheduleIndex(), WithStateMachine())
	f.Seed("https://a.example/soon", "a.example", 0.9, 0, 5, 10)
	f.Seed("https://b.example/soon", "b.example", 0.8, 0, 8, 10)
	f.Seed("https://a.example/later", "a.example", 0.7, 0, 9000, 10)
	f.Seed("https://b.example/later", "b.example", 0.6, 0, 9000, 10)

	due := f.DueURLs(100, 0, 0)
	if len(due) != 2 {
		t.Fatalf("due at hour 100 = %d, want 2", len(due))
	}
	if due[0].NextDue > due[1].NextDue {
		t.Fatalf("due not sorted by next_due: %d then %d", due[0].NextDue, due[1].NextDue)
	}
	if due[0].URL == "" {
		t.Fatal("due URL string is empty; the resident arena should resolve it")
	}

	onlyA := f.DueURLs(100, meguri.HostKeyOf("a.example"), 0)
	if len(onlyA) != 1 || onlyA[0].URL != "https://a.example/soon" {
		t.Fatalf("host-filtered due = %+v, want a.example/soon only", onlyA)
	}

	capped := f.DueURLs(100, 0, 1)
	if len(capped) != 1 {
		t.Fatalf("limited due = %d, want 1", len(capped))
	}
}

// sameKeySet reports whether two URLKey slices hold the same keys regardless of
// order, the comparison the wheel-vs-column equivalence needs.
func sameKeySet(a, b []meguri.URLKey) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[meguri.URLKey]int, len(a))
	for _, k := range a {
		seen[k]++
	}
	for _, k := range b {
		seen[k]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}
