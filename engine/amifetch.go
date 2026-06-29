package engine

import (
	"context"
	"strings"
	"time"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/fetch"
	"golang.org/x/net/html"
)

// amifetch.go is the one piece of code that knows both the ami fetch surface and
// the meguri engine (doc 13, the amifetch adapter). ami owns the network: DNS, the
// keep-alive transports, the per-host and per-IP limits, the adaptive timeout, the
// dead-host breaker. meguri owns the frontier: what to fetch next, when it is
// polite, and what an outcome means. The adapter is the seam between them. It turns
// a fetch.Request into the request ami runs and maps ami's result back into the
// meguri.Outcome the frontier folds, including extracting and keying the out-links
// the prioritizer spreads cash over. The live binding (an AmiClient backed by
// ami's fetch.Fetcher) is the only part that opens a socket; everything the adapter
// itself does, the request build and the result mapping, runs in-process and is
// gated by an AmiClient double that serves canned responses.

// AmiRequest is what the adapter hands an AmiClient: a single URL to fetch with the
// conditional-GET validators and the resolved IP the politeness bucket accounted
// for. It is the meguri-shaped view of an ami seed, decoupled from ami's own types
// so the engine never imports ami; the live binding translates it into ami's
// request, an offline double answers it from a fixture.
type AmiRequest struct {
	URL         string   // the URL to fetch
	IfNoneMatch string   // prior ETag, for a conditional GET; empty for unconditional
	IfModified  uint32   // prior Last-Modified epoch-hours, for If-Modified-Since; 0 if none
	ResolvedIP  [16]byte // the host's cached DNS answer, zero when ami must resolve
	Robots      bool     // this is the host's robots.txt fetch, body returned raw
}

// AmiResult is the meguri-shaped view of ami's fetch.Result: the fields the adapter
// maps into a meguri.Outcome. It mirrors ami's Result (Status, Header validators,
// Body, the post-fetch digest match, the redirect-resolved FinalURL, the server IP
// and timing) without importing it, so the live binding fills it from ami and a
// double fills it from a fixture. A transport failure that yields no HTTP exchange
// at all is Err; an HTTP error status is a normal result with that status.
type AmiResult struct {
	Status       int    // the HTTP status; 0 with Err set means no exchange
	Body         []byte // the response body, empty on a 304 or a no-body status
	ETag         string // the response ETag header, empty if none
	LastModified string // the response Last-Modified header, raw, empty if none
	Unchanged    bool   // ami's post-fetch digest matched the prior crawl (304-equivalent)
	FinalURL     string // the URL after ami followed redirects; equals the request URL if none
	LatencyMS    uint16 // round-trip latency in milliseconds
	RetryAfter   uint16 // deciseconds from a Retry-After header, 0 if none
	CrawlDelay   uint16 // deciseconds, a robots Crawl-delay when this was a robots fetch
	Err          error  // transport/DNS/timeout failure, no usable HTTP outcome
}

// AmiClient is the network seam: the one method the adapter calls to actually
// retrieve bytes. The production implementation wraps ami's fetch.Fetcher (the live
// binding); a test serves canned AmiResults so the whole adapter, the request build
// and the result mapping and the link extraction, runs without a socket. It must be
// safe for concurrent use: the engine dispatches many requests at once.
type AmiClient interface {
	Do(ctx context.Context, req AmiRequest) (AmiResult, error)
}

// AmiFetcher adapts an AmiClient to the engine's fetch.Fetcher SPI. It is the
// production Fetcher once its client is the live ami binding, and the same type the
// double drives in the gate. It carries the canonicalization policy and host
// grouping the out-link extraction keys under, the same ones the seed path uses, so
// a link and a seed enter the frontier on one keying.
type AmiFetcher struct {
	client   AmiClient
	grouping meguri.HostGrouping
	pol      *dedup.CanonPolicy
	now      func() uint32 // epoch-hours stamp for the outcome; wall clock by default
}

// AmiOption configures an AmiFetcher.
type AmiOption func(*AmiFetcher)

// WithAmiGrouping sets the host grouping out-links key under. The default is the
// registrable domain, the unit a partition owns and the seed path keys under.
func WithAmiGrouping(g meguri.HostGrouping) AmiOption {
	return func(a *AmiFetcher) { a.grouping = g }
}

// WithAmiCanonPolicy sets the link canonicalization policy. The default (nil) is
// the global default policy, the same one the seed reader uses.
func WithAmiCanonPolicy(p *dedup.CanonPolicy) AmiOption {
	return func(a *AmiFetcher) { a.pol = p }
}

// WithAmiClock sets the epoch-hours stamp source for the outcome's FetchedAt and
// the out-links' ObservedAt, so a replay can stamp deterministically. The default
// reads the wall clock.
func WithAmiClock(now func() uint32) AmiOption {
	return func(a *AmiFetcher) { a.now = now }
}

// NewAmiFetcher builds the adapter over a client. With the live ami binding as the
// client it is the production fetcher; with a fixture-serving double it is the
// in-process gate.
func NewAmiFetcher(client AmiClient, opts ...AmiOption) *AmiFetcher {
	a := &AmiFetcher{
		client:   client,
		grouping: meguri.GroupRegistrableDomain,
		now:      wallEpochHours,
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// wallEpochHours is the default outcome clock: the wall time in epoch-hours, the
// data-model unit.
func wallEpochHours() uint32 { return uint32(time.Now().Unix() / 3600) }

// Fetch runs one request through the ami client and maps its result into the
// outcome the frontier folds. A transport failure (no HTTP exchange) returns the
// error, which the engine turns into a retryable no-op; an HTTP error status is a
// normal outcome carrying that status. A robots fetch returns the raw body for the
// frontier to parse and never extracts links. A 304 or a digest match is a
// no-change outcome with the content signals zeroed, as the freshness model reads
// it. A 2xx extracts, canonicalizes, and keys the body's out-links so the
// prioritizer can spread the source's cash over them.
func (a *AmiFetcher) Fetch(ctx context.Context, req fetch.Request) (meguri.Outcome, error) {
	res, err := a.client.Do(ctx, AmiRequest{
		URL:         req.CanonicalURL,
		IfNoneMatch: req.ETag,
		IfModified:  req.LastModified,
		ResolvedIP:  req.ResolvedIP,
		Robots:      req.Robots,
	})
	if err != nil {
		return meguri.Outcome{}, err
	}
	if res.Err != nil {
		return meguri.Outcome{}, res.Err
	}

	now := a.now()
	o := meguri.Outcome{
		URLKey:       req.URLKey,
		HTTPStatus:   uint16(res.Status),
		FetchedAt:    now,
		LatencyMS:    res.LatencyMS,
		ETag:         res.ETag,
		LastModified: parseHTTPHours(res.LastModified),
		RetryAfter:   res.RetryAfter,
	}
	o.Retryable = res.Status == 429 || (res.Status >= 500 && res.Status <= 599)

	// A robots fetch hands back the raw body and a fresh Crawl-delay; the frontier
	// turns the bytes into rules. No link extraction: robots.txt is not content.
	if req.Robots {
		o.RobotsBody = res.Body
		o.RobotsCrawlDelay = res.CrawlDelay
		return o, nil
	}

	// A 304, or ami's post-fetch digest match, is a no-change observation: the
	// content signals stay zero, which freshness reads as no change (doc 13, the SPI
	// note that NotModified zeroes fp/simhash/len).
	if res.Status == 304 || res.Unchanged {
		o.NotModified = true
		return o, nil
	}

	// An explicit redirect status names a target the frontier creates and points the
	// source at; the body, if any, is the redirector's, not content to spread. When
	// ami transparently followed a redirect chain to a final 2xx, the redirect is
	// already resolved and the final URL is the content, so that case is a normal
	// crawl below, not a redirect outcome.
	if res.Status >= 300 && res.Status < 400 {
		o.RedirectTarget = canonOf(res.FinalURL, req.CanonicalURL, a.pol)
		return o, nil
	}

	// A content response: fingerprint the body, sign it for near-dup, record its
	// length, and extract the out-links the prioritizer spreads cash over. The links
	// are keyed against the final URL the body was served from, so a relative link on
	// a redirected page resolves correctly.
	if len(res.Body) > 0 {
		norm := dedup.NormalizeBody(res.Body)
		o.ContentFP = dedup.ContentFP(norm)
		o.Simhash = dedup.SimhashText(string(res.Body))
		o.ContentLen = uint32(len(res.Body))
		base := res.FinalURL
		if base == "" {
			base = req.CanonicalURL
		}
		o.Links = a.toDiscoveries(res.Body, base, req.Depth, now)
	}
	return o, nil
}

// toDiscoveries parses the out-links from an HTML body and turns each into a keyed
// discovery, the candidate the frontier's idempotent intake folds. It resolves each
// href against the base, canonicalizes and keys it under the same policy the seed
// path uses, and drops anything that is not a crawlable http(s) URL. Candidate depth
// is the source's depth plus one (saturating), the link distance the admission
// budget reads; the frontier re-stamps the source host and the per-link cash, so
// those are left zero here. Duplicate keys within one page collapse to one
// discovery, the cheap first cut before the receiver's seen-set dedups across pages.
func (a *AmiFetcher) toDiscoveries(body []byte, base string, srcDepth uint16, now uint32) []meguri.Discovery {
	depth := srcDepth
	if depth < ^uint16(0) { // saturate at the cap rather than wrap to zero
		depth++
	}
	var out []meguri.Discovery
	seen := map[meguri.URLKey]struct{}{}
	z := html.NewTokenizer(strings.NewReader(string(body)))
	for {
		switch z.Next() {
		case html.ErrorToken:
			return out
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			tag := string(name)
			if tag != "a" && tag != "area" {
				continue
			}
			if !hasAttr {
				continue
			}
			href, ok := hrefOf(z)
			if !ok {
				continue
			}
			key, canon, _, ok := dedup.CanonicalKey(href, base, a.grouping, a.pol)
			if !ok {
				continue
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, meguri.Discovery{
				URLKey:          key,
				CanonicalURL:    canon,
				Depth:           depth,
				DiscoverySource: meguri.SourceLink,
				ObservedAt:      now,
			})
		}
	}
}

// hrefOf returns the href attribute of the tag the tokenizer is positioned on, the
// only attribute the out-link extraction reads.
func hrefOf(z *html.Tokenizer) (string, bool) {
	for {
		k, v, more := z.TagAttr()
		if string(k) == "href" {
			return string(v), true
		}
		if !more {
			return "", false
		}
	}
}

// canonOf canonicalizes a redirect target against the source URL under the policy,
// returning the raw target unchanged when it does not canonicalize so the redirect
// is never silently dropped.
func canonOf(target, base string, pol *dedup.CanonPolicy) string {
	if canon, ok := dedup.Canonicalize(target, base, pol); ok {
		return canon
	}
	return target
}

// parseHTTPHours parses an HTTP Last-Modified header into epoch-hours, the
// data-model unit, returning zero for an empty or unparseable value. It accepts the
// three date forms RFC 7231 allows.
func parseHTTPHours(v string) uint32 {
	if v == "" {
		return 0
	}
	for _, layout := range []string{time.RFC1123, time.RFC1123Z, time.RFC850, time.ANSIC} {
		if t, err := time.Parse(layout, v); err == nil {
			return uint32(t.Unix() / 3600)
		}
	}
	return 0
}
