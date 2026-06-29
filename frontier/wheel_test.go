package frontier

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/tamnd/meguri"
)

func wkey(h, p uint64) meguri.URLKey { return meguri.URLKey{HostKey: h, PathKey: p} }

// TestWheelFiresInDueOrder checks the wheel holds a URL until its hour and then
// fires it, and that advancing past several hours fires every bucket the cursor
// passed exactly once.
func TestWheelFiresInDueOrder(t *testing.T) {
	w := newDueWheel(0)
	w.add(wkey(1, 1), 5)
	w.add(wkey(1, 2), 5)
	w.add(wkey(2, 1), 10)

	if got := w.due(4); len(got) != 0 {
		t.Fatalf("fired %d before any due hour, want 0", len(got))
	}
	got := w.due(5)
	if len(got) != 2 {
		t.Fatalf("fired %d at hour 5, want 2", len(got))
	}
	if got := w.due(9); len(got) != 0 {
		t.Fatalf("fired %d between due hours, want 0", len(got))
	}
	if got := w.due(10); len(got) != 1 {
		t.Fatalf("fired %d at hour 10, want 1", len(got))
	}
	if w.len() != 0 {
		t.Fatalf("wheel holds %d after everything fired, want 0", w.len())
	}
}

// TestWheelFiresOverdueOnce checks a key whose hour is already behind the cursor
// fires on the next advance, not lost, and only once.
func TestWheelFiresOverdueOnce(t *testing.T) {
	w := newDueWheel(100)
	w.add(wkey(1, 1), 50) // already overdue relative to the cursor
	got := w.due(100)
	if len(got) != 1 {
		t.Fatalf("fired %d for an overdue key, want 1", len(got))
	}
	if got := w.due(200); len(got) != 0 {
		t.Fatalf("re-fired an already-fired key %d times, want 0", len(got))
	}
}

// TestWheelOverflowFires checks a due time past the resident horizon waits in the
// overflow heap and still fires when the cursor advances to it.
func TestWheelOverflowFires(t *testing.T) {
	w := newDueWheel(0)
	far := uint32(wheelSpan + 200)
	w.add(wkey(1, 1), far)
	w.add(wkey(2, 1), 3) // a near one too, so the ring is not empty

	if got := w.due(3); len(got) != 1 {
		t.Fatalf("fired %d at the near hour, want 1", len(got))
	}
	if got := w.due(far - 1); len(got) != 0 {
		t.Fatalf("fired the overflow entry %d before its hour, want 0", len(got))
	}
	if got := w.due(far); len(got) != 1 {
		t.Fatalf("fired %d at the overflow hour, want 1", len(got))
	}
	if w.len() != 0 {
		t.Fatalf("wheel holds %d after the overflow fired, want 0", w.len())
	}
}

// TestWheelFastForwardEmptyHorizon checks advancing across a long empty stretch to
// a far-future overflow entry is cheap (it does not scan every hour) and still
// fires the entry at the right time. The assertion is on correctness; the
// fast-forward keeps the cost O(wheelSpan) rather than O(gap).
func TestWheelFastForwardEmptyHorizon(t *testing.T) {
	w := newDueWheel(0)
	far := uint32(50 * wheelSpan) // far beyond a single span, only reachable via the overflow tier
	w.add(wkey(7, 7), far)

	if got := w.due(far - 1); len(got) != 0 {
		t.Fatalf("fired %d before the far hour, want 0", len(got))
	}
	if d, ok := w.nextDue(); !ok || d != far {
		t.Fatalf("nextDue = (%d,%v), want (%d,true)", d, ok, far)
	}
	if got := w.due(far); len(got) != 1 {
		t.Fatalf("fired %d at the far hour, want 1", len(got))
	}
}

// TestWheelNextDue checks nextDue reports the earliest pending hour across both
// the ring and the overflow tier, the time the scheduler advances its clock to.
func TestWheelNextDue(t *testing.T) {
	w := newDueWheel(0)
	if _, ok := w.nextDue(); ok {
		t.Fatal("nextDue reported a time on an empty wheel")
	}
	w.add(wkey(1, 1), 40)
	w.add(wkey(2, 1), wheelSpan+5) // overflow
	w.add(wkey(3, 1), 12)          // earliest
	if d, ok := w.nextDue(); !ok || d != 12 {
		t.Fatalf("nextDue = (%d,%v), want (12,true)", d, ok)
	}
}

// TestWheelOnCorpus stages every distinct corpus URL into the wheel at a due hour
// spread across a real recrawl horizon, then drains it hour by hour and checks
// every key fires exactly once and in nondecreasing due order, the schedule index
// behaving on real keys at the corpus scale. It skips when no corpus is set.
func TestWheelOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(t, path)
	if len(keys) < 1000 {
		t.Skipf("corpus produced %d keys, need at least 1000", len(keys))
	}

	const base = 482817 // a real epoch-hour near the corpus observation window
	w := newDueWheel(base)
	want := map[meguri.URLKey]uint32{}
	var last uint32
	for _, k := range keys {
		// Spread the due hours over a flat recrawl gap so the ring and the overflow
		// tier both see real traffic, keyed deterministically off the path.
		due := base + uint32(k.PathKey%recrawlGapHours)
		w.add(k, due)
		want[k] = due
		if due > last {
			last = due
		}
	}
	if w.len() != len(keys) {
		t.Fatalf("wheel holds %d, staged %d", w.len(), len(keys))
	}

	seen := map[meguri.URLKey]bool{}
	var prevHour uint32
	fired := 0
	for h := uint32(base); h <= last; h++ {
		for _, k := range w.due(h) {
			if seen[k] {
				t.Fatalf("key %v fired twice", k)
			}
			seen[k] = true
			if want[k] != h {
				t.Fatalf("key %v fired at hour %d, due %d", k, h, want[k])
			}
			if h < prevHour {
				t.Fatalf("fired hour %d after %d, order went backward", h, prevHour)
			}
			prevHour = h
			fired++
		}
	}
	if fired != len(keys) {
		t.Fatalf("fired %d keys, staged %d", fired, len(keys))
	}
	if w.len() != 0 {
		t.Fatalf("wheel holds %d after draining, want 0", w.len())
	}
	t.Logf("schedule index on corpus: staged and fired %d distinct urls over a %d-hour horizon", fired, recrawlGapHours)
}

// loadCorpusKeys reads the corpus URLs into distinct URLKeys, the real host/path
// keys the wheel indexes, deriving the host and path the same way the rest of the
// corpus gates do.
func loadCorpusKeys(tb testing.TB, path string) []meguri.URLKey {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type rec struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	}
	seen := map[meguri.URLKey]bool{}
	var out []meguri.URLKey
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r rec
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		host := r.Host
		if host == "" {
			if _, after, ok := strings.Cut(r.URL, "://"); ok {
				host = after
			} else {
				host = r.URL
			}
			if i := strings.IndexAny(host, "/?#"); i >= 0 {
				host = host[:i]
			}
		}
		if host == "" {
			continue
		}
		p := "/"
		if _, after, ok := strings.Cut(r.URL, "://"); ok {
			if i := strings.IndexAny(after, "/?#"); i >= 0 {
				p = after[i:]
			}
		}
		key := meguri.MakeURLKey(host, p)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, key)
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return out
}
