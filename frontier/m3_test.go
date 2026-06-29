package frontier

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
)

// scriptFetcher is the M3 stand-in for ami: it replays a status per URL, hands
// back a robots body for a robots request, and records every request in dispatch
// order so a test can assert ordering and politeness. A zero status defaults to
// 200.
type scriptFetcher struct {
	status     map[meguri.URLKey]uint16 // per-URL HTTP status, 0 means 200
	robots     map[uint64][]byte        // per-host robots.txt body
	latencyMS  uint16
	retryAfter uint16
	etag       string
	lastMod    uint32
	notMod     bool
	calls      []fetch.Request
}

func (s *scriptFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	s.calls = append(s.calls, req)
	if req.Robots {
		return meguri.Outcome{
			URLKey:     req.URLKey,
			HTTPStatus: 200,
			RobotsBody: s.robots[req.HostKey],
		}, nil
	}
	st := uint16(200)
	if s.status != nil {
		if v, ok := s.status[req.URLKey]; ok {
			st = v
		}
	}
	return meguri.Outcome{
		URLKey:       req.URLKey,
		HTTPStatus:   st,
		ContentFP:    req.URLKey.PathKey | 1,
		LatencyMS:    s.latencyMS,
		RetryAfter:   s.retryAfter,
		ETag:         s.etag,
		LastModified: s.lastMod,
		NotModified:  s.notMod,
	}, nil
}

// poolResolver maps every host onto a small pool of IPs so many host groups
// share an address, the exact shape the per-IP bucket defends against. A pool of
// 1 puts every host on one machine.
type poolResolver struct{ pool uint32 }

func (r poolResolver) Resolve(_ context.Context, host string) ([16]byte, time.Duration, error) {
	return poolIP(host, r.pool), time.Hour, nil
}

// poolIP is the deterministic host-to-IP map the resolver and the test assertions
// share, so a test can recompute which dispatches landed on the same address.
func poolIP(host string, pool uint32) [16]byte {
	var h uint32 = 2166136261
	for i := 0; i < len(host); i++ {
		h ^= uint32(host[i])
		h *= 16777619
	}
	slot := h % pool
	var ip [16]byte
	ip[10], ip[11] = 0xff, 0xff // IPv4-mapped
	ip[12] = 10
	ip[13] = byte(slot >> 16)
	ip[14] = byte(slot >> 8)
	ip[15] = byte(slot)
	return ip
}

// TestPerIPSharesOneBucket is the per-IP guarantee: with twenty host groups all
// resolving to one address, no two fetches to that address ever land inside the
// per-IP interval, even though each host has its own host bucket open.
func TestPerIPSharesOneBucket(t *testing.T) {
	f := New(1, 0, WithResolver(poolResolver{pool: 1}))
	for h := range 20 {
		host := hostName(h)
		for u := range 4 {
			f.Seed("http://"+host+"/"+itoa(u), host, prioFor(h, u), 0, 0, 10)
		}
	}
	f.resolver.Wait() // drain the prefetch pool so every IP is resolved

	stream := drainFrom(t, f, 0)
	if len(stream) != 80 {
		t.Fatalf("dispatched %d, want 80", len(stream))
	}
	// One shared IP means a strictly serial schedule: each dispatch at least one
	// second after the last, never two at the same instant.
	for i := 1; i < len(stream); i++ {
		if stream[i].At <= stream[i-1].At {
			t.Fatalf("two fetches on one IP at %d then %d (step %d)", stream[i-1].At, stream[i].At, i)
		}
	}
}

// TestPerHostStillHoldsUnderSharedIP checks the host bucket is not lost when the
// IP bucket is the tighter one: a host is never dispatched twice inside its own
// interval regardless of how the IP throttle interleaves hosts.
func TestPerHostStillHoldsUnderSharedIP(t *testing.T) {
	f := New(1, 0, WithResolver(poolResolver{pool: 2}))
	const delay = uint16(30) // 3s host floor
	for h := range 12 {
		host := hostName(h)
		for u := range 3 {
			f.Seed("http://"+host+"/"+itoa(u), host, prioFor(h, u), 0, 0, delay)
		}
	}
	f.resolver.Wait()
	stream := drainFrom(t, f, 0)
	assertPoliteness(t, stream, delaySeconds(delay))
}

// TestRobotsExcludesDisallowed checks a host fetches robots before any content
// URL and that a disallowed path is excluded, not crawled.
func TestRobotsExcludesDisallowed(t *testing.T) {
	f := New(1, 0, WithRobots("meguri"))
	host := "ex.test"
	f.Seed("http://"+host+"/public/a", host, 0.9, 0, 0, 10)
	f.Seed("http://"+host+"/private/b", host, 0.8, 0, 0, 10)

	hk := meguri.HostKeyOf(host)
	body := []byte("User-agent: *\nDisallow: /private/\n")
	fr := &scriptFetcher{robots: map[uint64][]byte{hk: body}}

	out, err := f.Drain(context.Background(), 0, fr)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	if len(fr.calls) == 0 || !fr.calls[0].Robots {
		t.Fatalf("first fetch was not robots.txt: %+v", fr.calls)
	}
	// /public/a dispatches, /private/b never does (the robots fetch rides the
	// dispatch stream too, so strip it before counting content).
	content := stripRobots(out)
	if len(content) != 1 {
		t.Fatalf("dispatched %d content URLs, want 1 (the allowed one)", len(content))
	}
	allowed := meguri.MakeURLKey(host, "/public/a")
	denied := meguri.MakeURLKey(host, "/private/b")
	if content[0].Key != allowed {
		t.Errorf("dispatched the wrong URL: %v", content[0].Key)
	}
	if got := f.records[denied].Status; got != meguri.StatusExcludedRobots {
		t.Errorf("disallowed URL status = %v, want StatusExcludedRobots", got)
	}
}

// TestRobotsCrawlDelayRaisesFloor checks a robots Crawl-delay becomes the host's
// politeness floor: after robots, content fetches are spaced by at least it.
func TestRobotsCrawlDelayRaisesFloor(t *testing.T) {
	f := New(1, 0, WithRobots("meguri"))
	host := "slow.test"
	for u := range 3 {
		f.Seed("http://"+host+"/"+itoa(u), host, prioFor(0, u), 0, 0, 10) // 1s configured
	}
	hk := meguri.HostKeyOf(host)
	body := []byte("User-agent: *\nCrawl-delay: 5\n") // ask for 5s
	fr := &scriptFetcher{robots: map[uint64][]byte{hk: body}}

	out, err := f.Drain(context.Background(), 0, fr)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	// The robots fetch went out under the 1s default before its own Crawl-delay
	// was known, so assert the floor over content fetches only.
	assertPoliteness(t, stripRobots(out), 5) // 5s floor from robots beats the 1s config
}

// TestAIMDWidensOnError drives a host that keeps returning 503 and checks the
// gap between its fetches grows: the multiplicative backoff of AIMD on real
// status codes.
func TestAIMDWidensOnError(t *testing.T) {
	f := New(1, 0)
	host := "err.test"
	for u := range 4 {
		f.Seed("http://"+host+"/"+itoa(u), host, prioFor(0, u), 0, 0, 10)
	}
	keys := make([]meguri.URLKey, 4)
	status := map[meguri.URLKey]uint16{}
	for u := range 4 {
		keys[u] = meguri.MakeURLKey(host, "/"+itoa(u))
		status[keys[u]] = 503
	}
	fr := &scriptFetcher{status: status}

	out, err := f.Drain(context.Background(), 0, fr)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) < 3 {
		t.Fatalf("dispatched %d, want >= 3", len(out))
	}
	var gaps []uint32
	for i := 1; i < len(out); i++ {
		gaps = append(gaps, out[i].At-out[i-1].At)
	}
	// Each 503 backs the host off further, so the gaps are nondecreasing and the
	// last is strictly larger than the first.
	for i := 1; i < len(gaps); i++ {
		if gaps[i] < gaps[i-1] {
			t.Fatalf("backoff not monotone: gaps %v", gaps)
		}
	}
	if gaps[len(gaps)-1] <= gaps[0] {
		t.Fatalf("interval did not widen under sustained 503s: gaps %v", gaps)
	}
}

// TestConditionalGETStoresValidators checks a fetch's ETag and Last-Modified are
// stored so the next fetch of that URL goes conditional, and that a 304 is read
// as a no-change observation.
func TestConditionalGETStoresValidators(t *testing.T) {
	f := New(1, 0)
	host := "cond.test"
	url := "http://" + host + "/page"
	f.Seed(url, host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/page")

	// First fetch returns validators.
	req, ok := f.Dispatch(0)
	if !ok {
		t.Fatal("no dispatch")
	}
	if req.ETag != "" {
		t.Fatal("first fetch should carry no ETag")
	}
	f.Report(meguri.Outcome{URLKey: key, HTTPStatus: 200, ContentFP: 7, ETag: `"v1"`, LastModified: 100}, 0)

	rec := f.records[key]
	if f.arena.str(rec.ETagRef) != `"v1"` {
		t.Errorf("ETag not stored, got %q", f.arena.str(rec.ETagRef))
	}
	if rec.LastModified != 100 {
		t.Errorf("Last-Modified not stored, got %d", rec.LastModified)
	}

	// A 304 on the next fetch is a no-change observation.
	before := rec.NoChangeStreak
	f.Report(meguri.Outcome{URLKey: key, HTTPStatus: 304, NotModified: true}, 10)
	if rec.NoChangeStreak != before+1 {
		t.Errorf("304 did not bump NoChangeStreak: %d -> %d", before, rec.NoChangeStreak)
	}
}

// TestCorpusPolitenessNeverViolated is the M3 gate on real Common Crawl data: it
// seeds the frozen CC-MAIN-2026-25 slice, resolves every host onto a small IP
// pool, and replays the real HTTP status of each capture back through the
// scheduler. It then asserts the two hard politeness rules over the whole real
// dispatch stream (no host and no IP fetched inside its interval) and that every
// real 5xx folded into a host's error count, the AIMD backoff path running on
// real status codes.
func TestCorpusPolitenessNeverViolated(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	seeds := loadCorpusSeeds(t, path)
	if len(seeds) == 0 {
		t.Fatalf("corpus %s produced no seeds", path)
	}

	const pool = 8
	f := New(1, 0, WithResolver(poolResolver{pool: pool}))
	hostOf := map[meguri.URLKey]string{}
	status := map[meguri.URLKey]uint16{}
	for _, s := range seeds {
		f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
		key := meguri.MakeURLKey(s.host, PathOf(s.url))
		hostOf[key] = s.host
		status[key] = s.status
	}
	f.resolver.Wait()

	// Count 5xx over the deduped key set: seeding collapses repeated captures of a
	// URL into one record that dispatches once, so the per-key count is what the
	// host error totals must sum to.
	var errs int
	for _, st := range status {
		if st >= 500 && st <= 599 {
			errs++
		}
	}

	fr := &scriptFetcher{status: status}
	out, err := f.Drain(context.Background(), 0, fr)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Hard rule 1: no host group dispatched twice inside one second (the floor).
	lastHost := map[uint64]uint32{}
	for i, d := range out {
		if prev, ok := lastHost[d.HostKey]; ok && d.At < prev+1 {
			t.Fatalf("per-host interval violated at step %d: host fetched at %d then %d", i, prev, d.At)
		}
		lastHost[d.HostKey] = d.At
	}

	// Hard rule 2: no IP dispatched twice inside one second, recomputing the IP
	// each host resolved to.
	lastIP := map[[16]byte]uint32{}
	for i, d := range out {
		ip := poolIP(hostOf[d.Key], pool)
		if prev, ok := lastIP[ip]; ok && d.At < prev+1 {
			t.Fatalf("per-IP interval violated at step %d: IP fetched at %d then %d", i, prev, d.At)
		}
		lastIP[ip] = d.At
	}

	// Every real 5xx folded into AIMD: the error counts across hosts sum to the
	// number of 5xx captures in the slice.
	var folded uint32
	for _, h := range f.hosts {
		folded += h.rec.ErrorTotal
	}
	if int(folded) != errs {
		t.Errorf("AIMD saw %d server errors, corpus holds %d", folded, errs)
	}
	t.Logf("corpus: %d urls, %d dispatched, %d host groups on %d IPs, %d real 5xx fed to AIMD",
		len(seeds), len(out), len(f.hosts), pool, errs)
}

// --- M3 helpers ---

// stripRobots drops the robots.txt fetches from a dispatch stream, leaving only
// content URLs. A robots fetch rides the stream like any other dispatch, so a
// content-level assertion has to filter it first.
func stripRobots(stream []Dispatched) []Dispatched {
	out := stream[:0:0]
	for _, d := range stream {
		if d.Key == robotsKey(d.HostKey) {
			continue
		}
		out = append(out, d)
	}
	return out
}
