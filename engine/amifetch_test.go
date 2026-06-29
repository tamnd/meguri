package engine

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/fetch"
)

// scriptClient is the in-process AmiClient double: it answers each request from a
// function, standing in for the live ami fetch.Fetcher so the whole adapter (the
// request build, the result mapping, the link extraction) runs without a socket.
type scriptClient func(AmiRequest) (AmiResult, error)

func (c scriptClient) Do(_ context.Context, req AmiRequest) (AmiResult, error) { return c(req) }

const fixedHour = 466000 // a stable epoch-hours stamp so the gate is deterministic

func amiFor(t *testing.T, fn scriptClient) *AmiFetcher {
	t.Helper()
	return NewAmiFetcher(fn, WithAmiClock(func() uint32 { return fixedHour }))
}

// TestAmiFetcherContentResponse maps a 2xx body into an outcome: the content
// signals are set, the Last-Modified header is parsed to epoch-hours, and the
// out-links are extracted, canonicalized, keyed, deduped, and stamped at the
// source depth plus one.
func TestAmiFetcherContentResponse(t *testing.T) {
	body := []byte(`<html><body>
		<a href="/a">A</a>
		<a href="https://other.example/x?utm_source=spam">X</a>
		<a href="/a#frag">dup of A</a>
		<a href="mailto:nobody@example.com">skip</a>
		<area href="/b">B</area>
	</body></html>`)
	a := amiFor(t, func(req AmiRequest) (AmiResult, error) {
		return AmiResult{
			Status:       200,
			Body:         body,
			ETag:         "\"v2\"",
			LastModified: "Mon, 02 Jan 2006 15:04:05 GMT",
			FinalURL:     req.URL,
			LatencyMS:    42,
		}, nil
	})

	req := fetch.Request{
		URLKey:       meguri.URLKey{HostKey: 7, PathKey: 9},
		HostKey:      7,
		CanonicalURL: "https://h.example/page",
		Depth:        3,
	}
	o, err := a.Fetch(context.Background(), req)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if o.HTTPStatus != 200 || o.LatencyMS != 42 || o.ETag != "\"v2\"" {
		t.Fatalf("status/latency/etag = %d/%d/%q", o.HTTPStatus, o.LatencyMS, o.ETag)
	}
	if o.ContentFP == 0 || o.Simhash == 0 || o.ContentLen != uint32(len(body)) {
		t.Fatalf("content signals fp=%d simhash=%d len=%d, want all set", o.ContentFP, o.Simhash, o.ContentLen)
	}
	if o.LastModified == 0 {
		t.Fatal("last-modified did not parse to epoch-hours")
	}
	// /a (deduped), other.example/x (tracking param stripped), /b: three unique links.
	if len(o.Links) != 3 {
		t.Fatalf("extracted %d links, want 3: %+v", len(o.Links), o.Links)
	}
	for _, d := range o.Links {
		if d.Depth != 4 {
			t.Fatalf("link depth = %d, want 4 (source 3 + 1)", d.Depth)
		}
		if d.DiscoverySource != meguri.SourceLink {
			t.Fatalf("link source = %v, want SourceLink", d.DiscoverySource)
		}
		if d.ObservedAt != fixedHour {
			t.Fatalf("link observed-at = %d, want %d", d.ObservedAt, fixedHour)
		}
		if d.CanonicalURL == "" || d.URLKey == (meguri.URLKey{}) {
			t.Fatalf("link not keyed: %+v", d)
		}
	}
	// The relative link resolved against the base host, and the tracking parameter
	// was stripped by the canon policy, so the extraction keys exactly as a seed
	// would.
	wantA, _, _, _ := dedup.CanonicalKey("https://h.example/a", "", meguri.GroupRegistrableDomain, nil)
	found := false
	for _, d := range o.Links {
		if d.URLKey == wantA {
			found = true
		}
	}
	if !found {
		t.Fatal("relative link /a did not resolve to https://h.example/a")
	}
}

// TestAmiFetcherNotModified maps both a 304 and ami's post-fetch digest match to a
// no-change outcome with the content signals zeroed, as the freshness model reads.
func TestAmiFetcherNotModified(t *testing.T) {
	for _, tc := range []struct {
		name string
		res  AmiResult
	}{
		{"status-304", AmiResult{Status: 304, ETag: "\"v1\""}},
		{"digest-match", AmiResult{Status: 200, Unchanged: true, Body: []byte("unchanged body")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := amiFor(t, func(AmiRequest) (AmiResult, error) { return tc.res, nil })
			o, err := a.Fetch(context.Background(), fetch.Request{CanonicalURL: "https://h.example/p"})
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}
			if !o.NotModified {
				t.Fatal("not marked NotModified")
			}
			if o.ContentFP != 0 || o.Simhash != 0 || o.ContentLen != 0 || len(o.Links) != 0 {
				t.Fatalf("content signals not zeroed on no-change: fp=%d simhash=%d len=%d links=%d",
					o.ContentFP, o.Simhash, o.ContentLen, len(o.Links))
			}
		})
	}
}

// TestAmiFetcherRedirect maps an explicit 3xx to a redirect outcome carrying the
// canonicalized target, with no link extraction off the redirector body.
func TestAmiFetcherRedirect(t *testing.T) {
	a := amiFor(t, func(AmiRequest) (AmiResult, error) {
		return AmiResult{Status: 301, FinalURL: "https://h.example/new?utm_campaign=x"}, nil
	})
	o, err := a.Fetch(context.Background(), fetch.Request{CanonicalURL: "https://h.example/old"})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if o.RedirectTarget != "https://h.example/new" {
		t.Fatalf("redirect target = %q, want https://h.example/new (canonicalized)", o.RedirectTarget)
	}
	if len(o.Links) != 0 {
		t.Fatalf("redirect outcome extracted %d links, want 0", len(o.Links))
	}
}

// TestAmiFetcherErrorStatuses maps a transient 5xx and a 429-with-Retry-After to
// retryable outcomes that still carry their status, the back-off signal the state
// machine reads.
func TestAmiFetcherErrorStatuses(t *testing.T) {
	a := amiFor(t, func(req AmiRequest) (AmiResult, error) {
		if req.URL == "https://h.example/throttled" {
			return AmiResult{Status: 429, RetryAfter: 50}, nil
		}
		return AmiResult{Status: 503}, nil
	})
	o, err := a.Fetch(context.Background(), fetch.Request{CanonicalURL: "https://h.example/throttled"})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if o.HTTPStatus != 429 || !o.Retryable || o.RetryAfter != 50 {
		t.Fatalf("429 outcome = status %d retryable %v retry-after %d", o.HTTPStatus, o.Retryable, o.RetryAfter)
	}
	o, err = a.Fetch(context.Background(), fetch.Request{CanonicalURL: "https://h.example/down"})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if o.HTTPStatus != 503 || !o.Retryable {
		t.Fatalf("503 outcome = status %d retryable %v", o.HTTPStatus, o.Retryable)
	}
}

// TestAmiFetcherTransportError surfaces a transport failure (no HTTP exchange) as a
// Go error, both when the client itself errors and when the result carries Err, so
// the engine turns it into a retryable no-op rather than a bogus outcome.
func TestAmiFetcherTransportError(t *testing.T) {
	boom := errors.New("dial tcp: timeout")
	for _, tc := range []struct {
		name string
		fn   scriptClient
	}{
		{"client-error", func(AmiRequest) (AmiResult, error) { return AmiResult{}, boom }},
		{"result-err", func(AmiRequest) (AmiResult, error) { return AmiResult{Err: boom}, nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := amiFor(t, tc.fn)
			if _, err := a.Fetch(context.Background(), fetch.Request{CanonicalURL: "https://h.example/p"}); err == nil {
				t.Fatal("transport failure did not surface as an error")
			}
		})
	}
}

// TestAmiFetcherRobots maps a robots.txt fetch to an outcome carrying the raw body
// and a fresh Crawl-delay for the frontier to parse, never extracting links.
func TestAmiFetcherRobots(t *testing.T) {
	robots := []byte("User-agent: *\nCrawl-delay: 5\nDisallow: /private\n")
	a := amiFor(t, func(req AmiRequest) (AmiResult, error) {
		if !req.Robots {
			t.Errorf("robots request did not set the Robots flag")
		}
		return AmiResult{Status: 200, Body: robots, CrawlDelay: 50}, nil
	})
	o, err := a.Fetch(context.Background(), fetch.Request{CanonicalURL: "https://h.example/robots.txt", Robots: true})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(o.RobotsBody) != string(robots) {
		t.Fatalf("robots body = %q", o.RobotsBody)
	}
	if o.RobotsCrawlDelay != 50 {
		t.Fatalf("robots crawl-delay = %d, want 50", o.RobotsCrawlDelay)
	}
	if len(o.Links) != 0 {
		t.Fatalf("robots fetch extracted %d links, want 0", len(o.Links))
	}
}

// TestAmiFetcherDrainsCorpus is the at-scale binding: it seeds a frontier from the
// frozen corpus and drives the engine through the AmiFetcher, whose in-process
// client serves every real URL a 200 with a body that links back to itself. So the
// full path (dispatch -> ami request -> result map -> HTML tokenize -> canonicalize
// -> key -> idempotent intake) runs on every one of the 142083 real URLs, while the
// self-link dedups against the seen-set so the crawl still drains. Only the socket
// the live ami binding would open is replaced by the function call.
func TestAmiFetcherDrainsCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice")
	}
	fr := seedFromCorpus(t, path)
	n := fr.Len()
	if n < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000", n)
	}

	var fetched, links atomic.Int64
	clk := NewLogicalClock(1_700_000_000)
	client := scriptClient(func(req AmiRequest) (AmiResult, error) {
		// A body that links to the page itself: extraction runs, the candidate keys
		// back to a URL already in the frontier, and the seen-set dedups it.
		body := []byte(`<html><body><a href="` + req.URL + `">self</a></body></html>`)
		return AmiResult{Status: 200, Body: body, FinalURL: req.URL}, nil
	})
	af := NewAmiFetcher(client, WithAmiClock(func() uint32 { return uint32(clk.Now() / 3600) }))
	counting := &countingFetcher{inner: af, fetched: &fetched, links: &links}

	eng := New(fr, Config{Fetcher: counting, Workers: 16, Clock: clk, UntilEmpty: true})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if p := fr.Pending(); p != 0 {
		t.Fatalf("pending %d after drain, want 0 (self-links should dedup, not grow the frontier)", p)
	}
	gotFetched, gotLinks := fetched.Load(), links.Load()
	if gotFetched < int64(n) {
		t.Fatalf("fetched %d, want at least %d (each seeded url once)", gotFetched, n)
	}
	if gotLinks < int64(n) {
		t.Fatalf("extracted %d links over %d fetches, want one self-link each", gotLinks, gotFetched)
	}
	t.Logf("ami adapter drained %d real urls through the engine, %d fetches, %d links extracted and deduped",
		n, gotFetched, gotLinks)
}

// countingFetcher wraps a Fetcher to count fetches and extracted links, the at-scale
// assertion that the adapter's extraction ran on every real URL.
type countingFetcher struct {
	inner   fetch.Fetcher
	fetched *atomic.Int64
	links   *atomic.Int64
}

func (c *countingFetcher) Fetch(ctx context.Context, req fetch.Request) (meguri.Outcome, error) {
	o, err := c.inner.Fetch(ctx, req)
	if err == nil {
		c.fetched.Add(1)
		c.links.Add(int64(len(o.Links)))
	}
	return o, err
}
