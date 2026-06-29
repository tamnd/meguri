package engine

import (
	"context"
	"sync"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/frontier"
)

// Seed is one entry of a seed list as ami hands it across the boundary (doc 13):
// a URL to crawl, an initial priority, the host grouping it keys under, and the
// validators a prior crawl left so the first fetch can go conditional. It is the
// producer side; SeedReader turns it into the meguri.Discovery the frontier's
// idempotent intake ingests and the prior-crawl state Warm stamps on the record.
type Seed struct {
	URL      string  // the seed URL, canonicalized and keyed by the reader
	Base     string  // optional base for resolving a relative seed; usually empty
	Priority float32 // initial importance, carried as the discovery's link weight

	PrevETag         string // ETag from an earlier crawl, for the first conditional GET
	PrevLastModified uint32 // Last-Modified epoch-hours from an earlier crawl
	PrevDigest       uint64 // content fingerprint from an earlier crawl

	Source     meguri.DiscoverySource // how it entered; zero means SourceSeed
	ObservedAt uint32                 // epoch-hours the seed was listed; zero means unknown
}

// SeedReader converts seeds into discoveries with a fixed canonicalization policy
// and host grouping, the one piece of code that knows both the seed shape and the
// frontier's keying. It canonicalizes each URL, keys it the same way a crawled
// out-link is keyed, and copies the seed's priority into the discovery's link
// weight so a seed and a link enter the prioritizer on the same scale (doc 13).
type SeedReader struct {
	grouping meguri.HostGrouping
	pol      *dedup.CanonPolicy
}

// SeedOption configures a SeedReader.
type SeedOption func(*SeedReader)

// WithGrouping sets the host grouping seeds key under. The default is the
// registrable domain, the same unit a partition owns.
func WithGrouping(g meguri.HostGrouping) SeedOption {
	return func(r *SeedReader) { r.grouping = g }
}

// WithCanonPolicy sets the canonicalization policy. The default (nil) is the
// global default policy: the tracking-parameter deny-list and no host folding.
func WithCanonPolicy(p *dedup.CanonPolicy) SeedOption {
	return func(r *SeedReader) { r.pol = p }
}

// NewSeedReader builds a reader with the default registrable-domain grouping and
// the global canonicalization policy.
func NewSeedReader(opts ...SeedOption) *SeedReader {
	r := &SeedReader{grouping: meguri.GroupRegistrableDomain}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Discovery converts one seed into a discovery, returning false when the URL does
// not canonicalize to a crawlable http(s) key. The discovery carries the seed's
// canonical URL inline, its priority as the link weight, and SourceSeed unless the
// seed names another source.
func (r *SeedReader) Discovery(s Seed) (meguri.Discovery, bool) {
	key, canon, _, ok := dedup.CanonicalKey(s.URL, s.Base, r.grouping, r.pol)
	if !ok {
		return meguri.Discovery{}, false
	}
	src := s.Source
	return meguri.Discovery{
		URLKey:          key,
		CanonicalURL:    canon,
		Depth:           0,
		DiscoverySource: src,
		LinkWeight:      s.Priority,
		ObservedAt:      s.ObservedAt,
	}, true
}

// Ingest reads a batch of seeds into a frontier: it converts each to a discovery,
// folds it through the idempotent intake, and warms the new record with the
// seed's prior-crawl validators so its first fetch goes conditional. It returns
// how many seeds entered as new schedulable URLs. Ingest assumes single-threaded
// access to the frontier (the caller holds the engine's run goroutine or has not
// started it yet), matching the single-writer rule.
func (r *SeedReader) Ingest(fr *frontier.Frontier, seeds []Seed, now uint32) int {
	added := 0
	for _, s := range seeds {
		d, ok := r.Discovery(s)
		if !ok {
			continue
		}
		if fr.Discover(d, now) {
			added++
		}
		fr.Warm(d.URLKey, s.PrevETag, s.PrevLastModified, s.PrevDigest)
	}
	return added
}

// MeguriSeedSource is the inverse integration of Engine.Run: instead of meguri
// pulling a fetcher, ami pulls meguri, the partition presenting the ami.SeedSource
// shape (doc 13). Next blocks until a URL is both due and polite and returns its
// request; ami fetches it and hands the outcome back through Report. Because the
// frontier is a single-writer structure and ami calls from many goroutines, a
// mutex serializes every Dispatch and Report, so the partition stays the only
// writer to its own state even under a concurrent puller.
type MeguriSeedSource struct {
	mu  sync.Mutex
	fr  *frontier.Frontier
	clk Clock
}

// NewSeedSource presents a frontier as an ami seed source. A nil clock means a
// wall clock; the gate passes a logical clock so a replay drains without waits.
func NewSeedSource(fr *frontier.Frontier, clk Clock) *MeguriSeedSource {
	if clk == nil {
		clk = WallClock{}
	}
	return &MeguriSeedSource{fr: fr, clk: clk}
}

// Next returns the next URL to fetch, blocking until a host is due and polite. It
// returns ok=false when the frontier is fully drained (no parked host remains) or
// the context is cancelled, the signal for the puller to stop. While every host
// is cooling down it advances the clock to the next politeness window rather than
// busy-waiting, the same wait Engine.Run performs.
func (s *MeguriSeedSource) Next(ctx context.Context) (fetch.Request, bool) {
	for {
		if ctx.Err() != nil {
			return fetch.Request{}, false
		}
		s.mu.Lock()
		now := s.clk.Now()
		if req, ok := s.fr.Dispatch(now); ok {
			s.mu.Unlock()
			return req, true
		}
		t, has := s.fr.NextEligible()
		s.mu.Unlock()
		if !has {
			return fetch.Request{}, false
		}
		s.clk.SleepUntil(ctx, t)
	}
}

// Report folds a fetched outcome back into the frontier under the same lock Next
// dispatches under, so the puller's completion handler and its next pull never
// race on the frontier.
func (s *MeguriSeedSource) Report(o meguri.Outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fr.Report(o, s.clk.Now())
}
