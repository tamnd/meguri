package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/frontier"
)

// recFetcher records what the engine dispatched and when, reading the dispatch
// instant from the same logical clock the engine drives so the recorded time is
// the clock instant the URL went out at. It returns a plain crawled outcome with
// no links, turning the run into a pure scheduler drain.
type recFetcher struct {
	clk *LogicalClock
	mu  sync.Mutex
	seq []dispatchRec
}

type dispatchRec struct {
	key  meguri.URLKey
	host uint64
	at   uint32
}

func (f *recFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	at := f.clk.Now()
	f.mu.Lock()
	f.seq = append(f.seq, dispatchRec{key: req.URLKey, host: req.HostKey, at: at})
	f.mu.Unlock()
	return meguri.Outcome{URLKey: req.URLKey, HTTPStatus: 200, FetchedAt: at / 3600}, nil
}

// TestEngineDrainsSmallFrontier is the cheap unit gate with no corpus: a handful
// of hosts seeded, run through the engine, must each dispatch exactly once and
// leave nothing pending.
func TestEngineDrainsSmallFrontier(t *testing.T) {
	fr := frontier.New(1, 0)
	hosts := []string{"a.example", "b.example", "c.example"}
	for _, h := range hosts {
		for i := range 4 {
			fr.Seed("https://"+h+"/p/"+string(rune('a'+i)), h, 0.5, 0, 0, 10)
		}
	}
	clk := NewLogicalClock(1_000_000)
	rf := &recFetcher{clk: clk}
	eng := New(fr, Config{Fetcher: rf, Workers: 4, Clock: clk, UntilEmpty: true})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(rf.seq); got != 12 {
		t.Fatalf("dispatched %d urls, want 12", got)
	}
	if p := fr.Pending(); p != 0 {
		t.Fatalf("pending %d after drain, want 0", p)
	}
	assertDispatchedOnce(t, rf)
	assertPolite(t, rf, 1)
}

// TestEngineMatchesDrain checks the concurrent engine dispatches the same set of
// URLs the synchronous Drain does on an identically seeded frontier, so the loop
// adds concurrency without changing what gets crawled.
func TestEngineMatchesDrain(t *testing.T) {
	seed := func(fr *frontier.Frontier) {
		for _, h := range []string{"x.example", "y.example", "z.example", "w.example"} {
			for i := range 7 {
				fr.Seed("https://"+h+"/a/"+string(rune('0'+i)), h, float32(i)/7, 0, 0, 10)
			}
		}
	}

	frEng := frontier.New(1, 0)
	seed(frEng)
	clk := NewLogicalClock(2_000_000)
	rf := &recFetcher{clk: clk}
	eng := New(frEng, Config{Fetcher: rf, Workers: 3, Clock: clk, UntilEmpty: true})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	engSet := keySet(rf.seq)

	frDrain := frontier.New(1, 0)
	seed(frDrain)
	out, err := frDrain.Drain(context.Background(), 2_000_000, &recFetcher{clk: NewLogicalClock(2_000_000)})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	drainSet := map[meguri.URLKey]bool{}
	for _, d := range out {
		drainSet[d.Key] = true
	}

	if len(engSet) != len(drainSet) {
		t.Fatalf("engine dispatched %d distinct, drain %d", len(engSet), len(drainSet))
	}
	for k := range drainSet {
		if !engSet[k] {
			t.Fatalf("engine missed a url the drain dispatched: %x", k)
		}
	}
}

// TestEngineOnCorpus is the M10 engine gate on real data: the frozen
// CC-MAIN-2026-25 slice seeded into a frontier, run through the concurrent engine
// under a logical clock, must dispatch every URL exactly once, never violate a
// host's politeness interval, and leave nothing pending. This proves the staged
// loop, not just the synchronous Drain, is correct at corpus scale.
func TestEngineOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice")
	}
	fr := seedFromCorpus(t, path)
	n := fr.Len()
	if n < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000", n)
	}

	clk := NewLogicalClock(1_700_000_000)
	rf := &recFetcher{clk: clk}
	eng := New(fr, Config{Fetcher: rf, Workers: 16, Clock: clk, UntilEmpty: true})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(rf.seq); got != n {
		t.Fatalf("dispatched %d urls, want %d (each url exactly once)", got, n)
	}
	if p := fr.Pending(); p != 0 {
		t.Fatalf("pending %d after drain, want 0", p)
	}
	assertDispatchedOnce(t, rf)
	assertPolite(t, rf, 1)
	t.Logf("engine drained %d real urls across %d hosts, %d dispatched, politeness intact",
		n, hostCount(rf), eng.Stats().Dispatched)
}

// assertDispatchedOnce fails if any URL appears more than once in the dispatch
// stream, the invariant that one URL is fetched once per run.
func assertDispatchedOnce(t *testing.T, rf *recFetcher) {
	t.Helper()
	seen := make(map[meguri.URLKey]bool, len(rf.seq))
	for _, r := range rf.seq {
		if seen[r.key] {
			t.Fatalf("url dispatched twice: %x", r.key)
		}
		seen[r.key] = true
	}
}

// assertPolite fails if any host's consecutive dispatches are closer than
// minInterval seconds apart, the per-host politeness floor.
func assertPolite(t *testing.T, rf *recFetcher, minInterval uint32) {
	t.Helper()
	byHost := map[uint64][]uint32{}
	for _, r := range rf.seq {
		byHost[r.host] = append(byHost[r.host], r.at)
	}
	for host, times := range byHost {
		slices.Sort(times)
		for i := 1; i < len(times); i++ {
			if times[i]-times[i-1] < minInterval {
				t.Fatalf("host %x dispatched at %d then %d, under the %ds floor",
					host, times[i-1], times[i], minInterval)
			}
		}
	}
}

func keySet(seq []dispatchRec) map[meguri.URLKey]bool {
	s := make(map[meguri.URLKey]bool, len(seq))
	for _, r := range seq {
		s[r.key] = true
	}
	return s
}

func hostCount(rf *recFetcher) int {
	h := map[uint64]bool{}
	for _, r := range rf.seq {
		h[r.host] = true
	}
	return len(h)
}

// seedFromCorpus seeds a frontier from the frozen ccrawl jsonl slice, the same
// path the seed CLI takes, so the engine gate runs on the real corpus.
func seedFromCorpus(t *testing.T, path string) *frontier.Frontier {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open corpus: %v", err)
	}
	defer f.Close()
	type rec struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	}
	fr := frontier.New(1, 0)
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
			host = frontier.HostOf(r.URL)
		}
		if host == "" {
			continue
		}
		fr.Seed(r.URL, host, 0.5, 0, 0, 10)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan corpus: %v", err)
	}
	return fr
}
