package frontier

import (
	"context"
	"os"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/fetch"
)

// statusFetcher replays one HTTP status for every content URL, optionally marking
// it transient (Retryable) and optionally returning a redirect target. It is the
// minimal ami stand-in the outcome-state-machine tests drive.
type statusFetcher struct {
	status    uint16
	retryable bool
	redirect  string
}

func (s statusFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	return meguri.Outcome{
		URLKey:         req.URLKey,
		HTTPStatus:     s.status,
		FetchedAt:      100,
		ContentFP:      req.URLKey.PathKey | 1,
		Retryable:      s.retryable,
		RedirectTarget: s.redirect,
	}, nil
}

// TestStateMachineRetriesThenGone drives a host that always returns a transient
// 503: the URL is re-queued and re-dispatched until it hits the retry limit, then
// it tombstones to Gone and leaves the frontier. It is never counted as crawled.
func TestStateMachineRetriesThenGone(t *testing.T) {
	f := New(1, 0, WithStateMachine())
	host := "down.test"
	url := "http://" + host + "/p"
	f.Seed(url, host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/p")

	out, err := f.Drain(context.Background(), 0, statusFetcher{status: 503})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != int(maxRetries) {
		t.Fatalf("dispatched %d times, want %d (one per retry up to the limit)", len(out), maxRetries)
	}
	rec := f.records[key]
	if rec.Status != meguri.StatusGone {
		t.Fatalf("status = %v, want Gone after the retry budget", rec.Status)
	}
	if rec.RetryCount != maxRetries {
		t.Fatalf("retry_count = %d, want %d", rec.RetryCount, maxRetries)
	}
	if rec.ErrorCount != uint16(maxRetries) {
		t.Fatalf("error_count = %d, want %d", rec.ErrorCount, maxRetries)
	}
	if rec.CrawlCount != 0 {
		t.Fatalf("crawl_count = %d, a failed fetch must not count as a crawl", rec.CrawlCount)
	}
	if f.Pending() != 0 {
		t.Fatalf("pending %d, the tombstoned URL should leave the frontier", f.Pending())
	}
}

// TestStateMachine410Gone checks a 410 tombstones on the first failure, no
// retries: the server said the URL is permanently gone.
func TestStateMachine410Gone(t *testing.T) {
	f := New(1, 0, WithStateMachine())
	host := "gone.test"
	f.Seed("http://"+host+"/x", host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/x")

	out, err := f.Drain(context.Background(), 0, statusFetcher{status: 410})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("dispatched %d times, want 1 (410 is an immediate tombstone)", len(out))
	}
	if rec := f.records[key]; rec.Status != meguri.StatusGone {
		t.Fatalf("status = %v, want Gone on a 410", rec.Status)
	}
}

// TestStateMachine404ReprobeThenGone checks a 404 is re-probed (a page can come
// back) and tombstones only after the retry budget, unlike a 410.
func TestStateMachine404ReprobeThenGone(t *testing.T) {
	f := New(1, 0, WithStateMachine())
	host := "missing.test"
	f.Seed("http://"+host+"/y", host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/y")

	out, err := f.Drain(context.Background(), 0, statusFetcher{status: 404})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != int(maxRetries) {
		t.Fatalf("dispatched %d times, want %d (404 re-probes up to the limit)", len(out), maxRetries)
	}
	if rec := f.records[key]; rec.Status != meguri.StatusGone {
		t.Fatalf("status = %v, want Gone after the re-probe budget", rec.Status)
	}
}

// TestStateMachineRetryableTransportError checks the Retryable flag drives the
// retry path even with no HTTP status: a transport-level transient (a timeout, a
// reset) that ami classifies as transient is retried, not tombstoned on sight.
func TestStateMachineRetryableTransportError(t *testing.T) {
	f := New(1, 0, WithStateMachine())
	host := "timeout.test"
	f.Seed("http://"+host+"/z", host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/z")

	out, err := f.Drain(context.Background(), 0, statusFetcher{status: 0, retryable: true})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != int(maxRetries) {
		t.Fatalf("dispatched %d times, want %d", len(out), maxRetries)
	}
	if rec := f.records[key]; rec.Status != meguri.StatusGone {
		t.Fatalf("status = %v, want Gone after the retry budget", rec.Status)
	}
}

// TestStateMachineRedirect checks a redirect resolves the source (Crawled), points
// its redirect_ref at the canonical target, and creates the target as its own
// record on the right host so it gets crawled in turn.
func TestStateMachineRedirect(t *testing.T) {
	f := New(1, 0, WithStateMachine())
	host := "src.test"
	src := "http://" + host + "/old"
	f.Seed(src, host, 0.9, 0, 0, 10)
	srcKey := meguri.MakeURLKey(host, "/old")

	target := "https://dst.test/new"
	keyB, canonB, _, ok := dedup.CanonicalKey(target, src, meguri.GroupRegistrableDomain, nil)
	if !ok {
		t.Fatal("target did not canonicalize")
	}

	// Dispatch the source and hand back a 301 to the target.
	req, ok := f.Dispatch(0)
	if !ok {
		t.Fatal("no dispatch")
	}
	if req.URLKey != srcKey {
		t.Fatalf("dispatched %x, want the source %x", req.URLKey, srcKey)
	}
	f.Report(statusFetcherOutcome(req.URLKey, 301, target), 0)

	srcRec := f.records[srcKey]
	if srcRec.Status != meguri.StatusCrawled {
		t.Fatalf("source status = %v, want Crawled (it resolved to a redirect)", srcRec.Status)
	}
	if f.arena.str(srcRec.RedirectRef) != canonB {
		t.Fatalf("redirect_ref = %q, want the canonical target %q", f.arena.str(srcRec.RedirectRef), canonB)
	}
	tgt := f.records[keyB]
	if tgt == nil {
		t.Fatal("redirect target was not created as its own record")
	}
	if tgt.DiscoverySource != meguri.SourceRedirect {
		t.Fatalf("target discovery source = %v, want SourceRedirect", tgt.DiscoverySource)
	}
	if tgt.Status != meguri.StatusScheduled {
		t.Fatalf("target status = %v, want Scheduled so it gets crawled", tgt.Status)
	}
}

// statusFetcherOutcome builds a redirect outcome for the manual Report path.
func statusFetcherOutcome(key meguri.URLKey, status uint16, redirect string) meguri.Outcome {
	return meguri.Outcome{URLKey: key, HTTPStatus: status, FetchedAt: 0, RedirectTarget: redirect}
}

// TestStateMachineOffMarksCrawled documents the gate-preserving default: with the
// state machine off, even a 503 marks the URL Crawled and folds once, the earlier
// milestones' behavior the M3 corpus gate depends on.
func TestStateMachineOffMarksCrawled(t *testing.T) {
	f := New(1, 0) // no WithStateMachine
	host := "legacy.test"
	f.Seed("http://"+host+"/p", host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/p")

	out, err := f.Drain(context.Background(), 0, statusFetcher{status: 503})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("dispatched %d times, want 1 (the legacy path does not retry)", len(out))
	}
	rec := f.records[key]
	if rec.Status != meguri.StatusCrawled {
		t.Fatalf("status = %v, want Crawled (the legacy always-crawled path)", rec.Status)
	}
	if rec.CrawlCount != 1 {
		t.Fatalf("crawl_count = %d, want 1", rec.CrawlCount)
	}
}

// TestStateMachineOnCorpus replays the real CC-MAIN-2026-25 statuses through the
// full state machine and asserts it drains to a clean terminal state on real
// data: every URL ends Crawled, Gone, or ExcludedRobots (nothing stuck Scheduled
// or InFlight), politeness is never violated, and the retry-then-tombstone path
// actually fires on the slice's real 4xx/5xx. It proves the state machine
// converges at corpus scale, not just on synthetic single-host cases.
func TestStateMachineOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice")
	}
	seeds := loadCorpusSeeds(t, path)
	if len(seeds) == 0 {
		t.Fatalf("corpus %s produced no seeds", path)
	}

	const pool = 8
	f := New(1, 0, WithResolver(poolResolver{pool: pool}), WithStateMachine())
	status := map[meguri.URLKey]uint16{}
	for _, s := range seeds {
		f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
		status[meguri.MakeURLKey(s.host, PathOf(s.url))] = s.status
	}
	f.resolver.Wait()

	out, err := f.Drain(context.Background(), 0, corpusStateFetcher{status: status})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Politeness still holds under the retry re-queues.
	lastHost := map[uint64]uint32{}
	for i, d := range out {
		if prev, ok := lastHost[d.HostKey]; ok && d.At < prev+1 {
			t.Fatalf("per-host interval violated at step %d: %d then %d", i, prev, d.At)
		}
		lastHost[d.HostKey] = d.At
	}

	// Every URL ends in a terminal state, and the failure path actually fired.
	var crawled, gone, excluded, other int
	for _, rec := range f.records {
		switch rec.Status {
		case meguri.StatusCrawled, meguri.StatusDueRecrawl:
			crawled++
		case meguri.StatusGone:
			gone++
		case meguri.StatusExcludedRobots:
			excluded++
		default:
			other++
		}
	}
	hasFailing := false
	for _, st := range status {
		if st >= 400 && st <= 599 {
			hasFailing = true
			break
		}
	}
	if other != 0 {
		t.Fatalf("%d URLs left in a non-terminal state after drain, want 0", other)
	}
	if hasFailing && gone == 0 {
		t.Fatalf("corpus has failing statuses but no URL tombstoned to Gone")
	}
	t.Logf("state machine drained %d urls: %d crawled, %d gone, %d excluded; %d dispatches",
		len(seeds), crawled, gone, excluded, len(out))
}

// corpusStateFetcher replays the real per-URL status and marks 429/5xx transient,
// the classification ami would hand back, so the state machine retries them.
type corpusStateFetcher struct {
	status map[meguri.URLKey]uint16
}

func (s corpusStateFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	if req.Robots {
		return meguri.Outcome{URLKey: req.URLKey, HTTPStatus: 200}, nil
	}
	st := uint16(200)
	if v, ok := s.status[req.URLKey]; ok && v != 0 {
		st = v
	}
	return meguri.Outcome{
		URLKey:     req.URLKey,
		HTTPStatus: st,
		FetchedAt:  100,
		ContentFP:  req.URLKey.PathKey | 1,
		Retryable:  st == 429 || (st >= 500 && st <= 599),
	}, nil
}
