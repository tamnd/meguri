package frontier

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/format"
)

// stubFetcher is the M1 fetcher: it returns a trivial success for every request
// without touching the network. The scheduler is what the gate exercises, so the
// outcome only needs to close the loop, not carry a real body. ami (網) is the
// production Fetcher; this stands in until M3 wires it.
type stubFetcher struct{ at uint32 }

func (s stubFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	return meguri.Outcome{
		URLKey:     req.URLKey,
		HTTPStatus: 200,
		FetchedAt:  s.at,
		ContentFP:  1,
	}, nil
}

// TestFrontBucketMonotone checks the front-bank bucket mapping is monotone in
// priority and pins the endpoints, so a higher-priority URL never lands in a
// lower bucket.
func TestFrontBucketMonotone(t *testing.T) {
	if got := frontBucket(0); got != 0 {
		t.Fatalf("frontBucket(0) = %d, want 0", got)
	}
	if got := frontBucket(1); got != priorityLevels-1 {
		t.Fatalf("frontBucket(1) = %d, want %d", got, priorityLevels-1)
	}
	prev := -1
	for p := float32(0); p <= 1.0001; p += 0.01 {
		b := frontBucket(p)
		if b < prev {
			t.Fatalf("bucket dropped: frontBucket(%.3f) = %d after %d", p, b, prev)
		}
		prev = b
	}
}

// TestPrioRingHighestFirst checks the ring always pops from the highest
// non-empty bucket and keeps insertion order within a bucket.
func TestPrioRingHighestFirst(t *testing.T) {
	var r prioRing[int]
	r.push(10, 0.1)
	r.push(20, 0.9)
	r.push(30, 0.9)
	r.push(40, 0.5)
	var got []int
	for {
		v, ok := r.pop()
		if !ok {
			break
		}
		got = append(got, v)
	}
	// 20 and 30 share the top bucket (insertion order), then 40, then 10.
	want := []int{20, 30, 40, 10}
	if len(got) != len(want) {
		t.Fatalf("popped %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("popped %v, want %v", got, want)
		}
	}
}

// TestPriorityThenPoliteness is the M1 gate on a hand-built input where the
// correct order is known: dispatch must prefer the higher-priority ready URL and
// must never fetch a host inside its minimum interval.
//
// Two hosts, two URLs each, distinct priorities chosen to fall in distinct
// buckets, with a 2-second crawl delay. Starting at t=0 every URL is schedulable
// and every host is eligible, so the first two dispatches are the two hosts'
// best URLs in global priority order. Their second URLs cannot go until each
// host's window reopens at t=2, again in priority order.
func TestPriorityThenPoliteness(t *testing.T) {
	f := New(1, 0)
	// host A: priorities 0.9 (best) and 0.3
	f.Seed("http://a.test/best", "a.test", 0.9, 0, 0, 20)
	f.Seed("http://a.test/low", "a.test", 0.3, 0, 0, 20)
	// host B: priorities 0.6 and 0.2
	f.Seed("http://b.test/mid", "b.test", 0.6, 0, 0, 20)
	f.Seed("http://b.test/least", "b.test", 0.2, 0, 0, 20)

	urls := dispatchURLs(t, f, 0)
	want := []string{
		"http://a.test/best",  // 0.9, t=0
		"http://b.test/mid",   // 0.6, t=0
		"http://a.test/low",   // 0.3, t=2 (A reopened)
		"http://b.test/least", // 0.2, t=2 (B reopened)
	}
	if len(urls) != len(want) {
		t.Fatalf("dispatched %v, want %v", urls, want)
	}
	for i := range want {
		if urls[i] != want[i] {
			t.Fatalf("dispatch %d = %q, want %q (full: %v)", i, urls[i], want[i], urls)
		}
	}
}

// TestPolitenessInterval drives many hosts and asserts the hard rule: no host is
// ever dispatched twice inside its crawl delay.
func TestPolitenessInterval(t *testing.T) {
	f := New(1, 0)
	const delayDecis = 30 // 3 seconds
	for h := range 20 {
		host := hostName(h)
		for u := range 5 {
			f.Seed("http://"+host+"/"+itoa(u), host, prioFor(h, u), 0, 0, delayDecis)
		}
	}
	stream := drain(t, f, 0)
	assertPoliteness(t, stream, delaySeconds(delayDecis))
}

// TestCheckpointRecoverIdenticalSequence is the M1 durability gate: a frontier
// dispatched part way, checkpointed to a .meguri file, and recovered must
// produce exactly the dispatch sequence the original would have continued with.
func TestCheckpointRecoverIdenticalSequence(t *testing.T) {
	seeds := syntheticSeeds(12, 6, 20)

	// Full reference run.
	ref := drain(t, seedAll(New(1, 0), seeds), 0)

	// Split run: dispatch K, checkpoint, recover, finish.
	f := seedAll(New(1, 0), seeds)
	const k = 17
	now := uint32(0)
	var got []Dispatched
	steps := 0
	fetcher := stubFetcher{}
	for steps < k {
		req, ok := f.Dispatch(now)
		if ok {
			got = append(got, Dispatched{Key: req.URLKey, HostKey: req.HostKey, At: now})
			o, _ := fetcher.Fetch(context.Background(), req)
			f.Report(o, now)
			steps++
			continue
		}
		t2, ok := f.NextEligible()
		if !ok {
			break
		}
		now = t2
	}

	raw, err := f.CheckpointBytes()
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	p, err := format.Decode(raw)
	if err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	rec := Recover(p)
	got = append(got, drainFrom(t, rec, now)...)

	if len(got) != len(ref) {
		t.Fatalf("recovered run length %d, reference %d", len(got), len(ref))
	}
	for i := range ref {
		if got[i].Key != ref[i].Key {
			t.Fatalf("dispatch %d diverged after recovery: got %x want %x", i, got[i].Key.Bytes(), ref[i].Key.Bytes())
		}
	}
}

// TestBoundedActiveHosts checks WithTarget bounds the number of resident back
// queues: with a small target the frontier still drains every URL, never
// exceeding the cap of active hosts at once.
func TestBoundedActiveHosts(t *testing.T) {
	seeds := syntheticSeeds(30, 4, 20)
	const target = 5
	f := seedAll(New(1, 0, WithTarget(target)), seeds)

	now := uint32(0)
	seen := 0
	fetcher := stubFetcher{}
	for {
		req, ok := f.Dispatch(now)
		if ok {
			if f.active > target {
				t.Fatalf("active hosts %d exceeded target %d", f.active, target)
			}
			o, _ := fetcher.Fetch(context.Background(), req)
			f.Report(o, now)
			seen++
			continue
		}
		t2, ok := f.NextEligible()
		if !ok {
			break
		}
		now = t2
	}
	if seen != len(seeds) {
		t.Fatalf("dispatched %d urls, seeded %d", seen, len(seeds))
	}
}

// TestCorpusGate is the M1 gate on real Common Crawl data: seed a frontier from
// a frozen ccrawl slice, drain it, and require both invariants on the real
// dispatch stream. No host is fetched inside its interval, and a checkpoint
// taken mid-run recovers to the identical remaining sequence. It skips when no
// corpus is configured.
func TestCorpusGate(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	seeds := loadCorpusSeeds(t, path)
	if len(seeds) == 0 {
		t.Fatalf("corpus %s produced no seeds", path)
	}
	const delay = uint32(1) // delaySeconds(10 deciseconds)

	ref := drain(t, seedAll(New(1, 0), seeds), 0)
	assertPoliteness(t, ref, delay)
	t.Logf("corpus: %d urls dispatched across the stream", len(ref))

	// Checkpoint at the midpoint and require an identical tail.
	f := seedAll(New(1, 0), seeds)
	half := len(ref) / 2
	now := uint32(0)
	var got []Dispatched
	fetcher := stubFetcher{}
	for len(got) < half {
		req, ok := f.Dispatch(now)
		if ok {
			got = append(got, Dispatched{Key: req.URLKey, HostKey: req.HostKey, At: now})
			o, _ := fetcher.Fetch(context.Background(), req)
			f.Report(o, now)
			continue
		}
		t2, ok := f.NextEligible()
		if !ok {
			break
		}
		now = t2
	}
	raw, err := f.CheckpointBytes()
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	p, err := format.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got = append(got, drainFrom(t, Recover(p), now)...)

	if len(got) != len(ref) {
		t.Fatalf("recovered run length %d, reference %d", len(got), len(ref))
	}
	for i := range ref {
		if got[i].Key != ref[i].Key {
			t.Fatalf("real-data dispatch %d diverged after recovery", i)
		}
	}
}

// --- helpers ---

type seed struct {
	url, host string
	priority  float32
	delay     uint16
	status    uint16 // real HTTP status from the corpus capture, 0 outside the corpus
}

func seedAll(f *Frontier, seeds []seed) *Frontier {
	for _, s := range seeds {
		f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
	}
	return f
}

// syntheticSeeds builds hosts*perHost deterministic seeds with spread
// priorities and a fixed crawl delay, sorted by URLKey so the front bank fills
// in a stable order.
func syntheticSeeds(hosts, perHost int, delay uint16) []seed {
	var out []seed
	for h := range hosts {
		host := hostName(h)
		for u := range perHost {
			out = append(out, seed{
				url:      "http://" + host + "/" + itoa(u),
				host:     host,
				priority: prioFor(h, u),
				delay:    delay,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ki := meguri.MakeURLKey(out[i].host, PathOf(out[i].url))
		kj := meguri.MakeURLKey(out[j].host, PathOf(out[j].url))
		return ki.Less(kj)
	})
	return out
}

func loadCorpusSeeds(tb testing.TB, path string) []seed {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type cdx struct {
		URL    string `json:"url"`
		Host   string `json:"host"`
		Status string `json:"status"`
	}
	var out []seed
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec cdx
		if json.Unmarshal([]byte(line), &rec) != nil || rec.URL == "" {
			continue
		}
		host := rec.Host
		if host == "" {
			host = HostOf(rec.URL)
		}
		if host == "" {
			continue
		}
		st := uint16(200) // a capture with no parseable status reads as a plain 200
		if n, err := strconv.Atoi(rec.Status); err == nil && n > 0 && n < 600 {
			st = uint16(n)
		}
		out = append(out, seed{url: rec.URL, host: host, priority: corpusPrio(rec.URL), delay: 10, status: st})
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	sort.Slice(out, func(i, j int) bool {
		ki := meguri.MakeURLKey(out[i].host, PathOf(out[i].url))
		kj := meguri.MakeURLKey(out[j].host, PathOf(out[j].url))
		return ki.Less(kj)
	})
	return out
}

// drain runs a frontier to exhaustion from t=0 and returns the dispatch stream.
func drain(tb testing.TB, f *Frontier, start uint32) []Dispatched {
	tb.Helper()
	return drainFrom(tb, f, start)
}

func drainFrom(tb testing.TB, f *Frontier, start uint32) []Dispatched {
	tb.Helper()
	out, err := f.Drain(context.Background(), start, stubFetcher{})
	if err != nil {
		tb.Fatalf("drain: %v", err)
	}
	return out
}

// dispatchURLs drains and resolves each dispatched key back to its URL text, for
// the small hand-built ordering test.
func dispatchURLs(tb testing.TB, f *Frontier, start uint32) []string {
	tb.Helper()
	stream := drainFrom(tb, f, start)
	var urls []string
	for _, d := range stream {
		urls = append(urls, f.arena.str(f.records[d.Key].URLRef))
	}
	return urls
}

// assertPoliteness checks no host appears twice within delay seconds.
func assertPoliteness(tb testing.TB, stream []Dispatched, delay uint32) {
	tb.Helper()
	last := map[uint64]uint32{}
	seen := map[uint64]bool{}
	for i, d := range stream {
		if prev, ok := last[d.HostKey]; ok && d.At < prev+delay {
			tb.Fatalf("politeness violated at step %d: host dispatched at %d then %d, delay %d", i, prev, d.At, delay)
		}
		last[d.HostKey] = d.At
		seen[d.HostKey] = true
	}
}

func hostName(h int) string { return "h" + itoa(h) + ".test" }

// prioFor spreads priorities deterministically so a host's URLs descend in
// importance and different hosts interleave.
func prioFor(h, u int) float32 {
	base := 1.0 - float32(u)*0.13
	jitter := float32((h*7)%11) * 0.01
	p := base - jitter
	if p <= 0 {
		p = 0.01
	}
	if p >= 1 {
		p = 0.99
	}
	return p
}

// corpusPrio derives a stable priority in (0,1) from a real URL, so the real
// dispatch stream has a genuine priority spread rather than a flat order.
func corpusPrio(u string) float32 {
	var s uint32 = 2166136261
	for i := 0; i < len(u); i++ {
		s ^= uint32(u[i])
		s *= 16777619
	}
	return float32(s%1000+1) / 1001.0
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
