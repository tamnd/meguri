// Package engine composes the resident frontier with a fetcher worker pool and
// the distribution router into the staged, single-writer partition loop of doc
// 04. The frontier is the only writer to its own state: every mutation, whether a
// routed-discovery intake or an outcome fold, runs on the engine's one run
// goroutine, while the fetchers run concurrently on a bounded pool sized to the
// polite-host parallelism. Out-links split at the fold, the frontier's link sink
// shipping the remote ones to their owners through the router and handing back the
// local ones for this partition's intake, exactly the doc 04 dataflow.
//
// Two integration directions ride the same frontier. Engine.Run is meguri driving
// ami: the engine pulls the next polite URL, hands it to the fetcher pool, and
// folds the outcome back. MeguriSeedSource is the inverse, ami driving meguri: a
// partition presents the ami.SeedSource shape, Next blocking until a URL is due
// and polite (doc 13). Both keep the single-writer rule by serializing every
// frontier call.
package engine

import (
	"context"
	"sync/atomic"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/distribute"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/frontier"
)

// Config wires an Engine. Only Fetcher is required; the rest take working
// defaults so a single-box replay run needs nothing more than a fetcher.
type Config struct {
	// Fetcher retrieves one request and returns its outcome; it must be safe for
	// concurrent use, since the pool calls it from every worker at once.
	Fetcher fetch.Fetcher
	// Workers is the polite-host parallelism, the in-flight fetch bound. Zero
	// means a small default that keeps a single box busy without oversubscribing
	// the fetcher.
	Workers int
	// Clock is the engine's time source. Zero means a wall clock for a live crawl;
	// the corpus gate passes a logical clock so it drains without real waits.
	Clock Clock
	// Router, when set, is the inbound side of distribution: the engine drains the
	// transport for discoveries routed to this partition and folds them into the
	// frontier. The outbound side is the frontier's own link sink (WithLinkRouter,
	// wired with RouteSink), so a partition both ships and receives cross-partition
	// links. Nil is the single-partition case.
	Router *distribute.Router
	// UntilEmpty stops Run when the frontier drains and no inbound work remains,
	// the mode the gate and a bounded batch crawl use. False runs until the context
	// is cancelled, the mode a live fleet partition uses, waiting on new
	// discoveries when its own frontier is momentarily empty.
	UntilEmpty bool
}

const defaultWorkers = 8

// Engine drives one partition's crawl loop. It owns no durable state of its own:
// the frontier holds the schedule, the fetcher pool holds the in-flight work, and
// the engine is the single goroutine that moves URLs between them.
type Engine struct {
	fr         *frontier.Frontier
	bf         *BatchFetcher
	clk        Clock
	router     *distribute.Router
	untilEmpty bool

	dispatched atomic.Int64
	fetched    atomic.Int64
	failed     atomic.Int64
}

// New builds an engine over an already-configured frontier. The frontier carries
// its own policy (politeness, freshness, prioritization) and, for a distributed
// run, its outbound link sink (frontier.WithLinkRouter wired with RouteSink); the
// engine adds the loop, the fetcher pool, the clock, and the inbound intake.
func New(fr *frontier.Frontier, cfg Config) *Engine {
	workers := cfg.Workers
	if workers < 1 {
		workers = defaultWorkers
	}
	clk := cfg.Clock
	if clk == nil {
		clk = WallClock{}
	}
	return &Engine{
		fr:         fr,
		bf:         NewBatchFetcher(cfg.Fetcher, workers),
		clk:        clk,
		router:     cfg.Router,
		untilEmpty: cfg.UntilEmpty,
	}
}

// Stats is a snapshot of what the engine has moved: URLs dispatched to the pool,
// outcomes folded back, and fetches that errored without a usable outcome.
type Stats struct {
	Dispatched int64
	Fetched    int64
	Failed     int64
}

// Stats returns a live snapshot, safe to call from another goroutine while Run
// is in flight.
func (e *Engine) Stats() Stats {
	return Stats{
		Dispatched: e.dispatched.Load(),
		Fetched:    e.fetched.Load(),
		Failed:     e.failed.Load(),
	}
}

// Run drives the crawl loop until the frontier drains (UntilEmpty) or the context
// is cancelled. It is the single writer to the frontier: it dispatches the next
// polite URL, places it on the fetcher pool, and folds each returned outcome back,
// all on this one goroutine, while the pool's workers fetch concurrently. When no
// host may be fetched and nothing is in flight, it advances the clock to the next
// politeness window rather than spinning.
func (e *Engine) Run(ctx context.Context) error {
	in, out := e.bf.stream(ctx)
	defer close(in) // drains the workers and closes out

	inflight := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := e.clk.Now()
		e.intake(now) // fold inbound routed discoveries before deciding what to dispatch

		now = e.clk.Now()
		if req, ok := e.fr.Dispatch(now); ok {
			if err := e.place(ctx, in, out, req, &inflight); err != nil {
				return err
			}
			continue
		}

		// Nothing dispatchable at this instant. Drain an in-flight outcome if one is
		// pending, since folding it may open a host or add a local discovery.
		if inflight > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case res := <-out:
				e.fold(res, &inflight)
			}
			continue
		}

		// Idle: nothing in flight and nothing to dispatch now. A late inbound
		// discovery may still have arrived, so check the intake once more before
		// committing to a wait.
		if e.intake(e.clk.Now()) {
			continue
		}
		if t, ok := e.fr.NextEligible(); ok {
			e.clk.SleepUntil(ctx, t) // wall clock waits, logical clock jumps
			continue
		}

		// The frontier is fully drained. A bounded run is done; a live partition
		// waits for the next inbound discovery, or for cancellation.
		if e.untilEmpty {
			return nil
		}
		if !e.blockForInbound(ctx) {
			return nil
		}
	}
}

// place hands one dispatched request to the fetcher pool. The request is already
// marked in-flight in the frontier, so it must reach a worker: while the pool is
// full the send blocks, and place folds whatever outcomes complete in the
// meantime, which both frees a worker and makes progress.
func (e *Engine) place(ctx context.Context, in chan<- fetch.Request, out <-chan Result, req fetch.Request, inflight *int) error {
	for {
		select {
		case in <- req:
			*inflight++
			e.dispatched.Add(1)
			return nil
		case res := <-out:
			e.fold(res, inflight)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// fold folds one finished fetch back into the frontier on the run goroutine. A
// usable outcome goes straight to Report, which updates the URL's state, its
// change-rate, its host's adaptive rate, and (through the frontier's link sink)
// routes its out-links. A fetch error yields no outcome, so the engine reports a
// transient one that clears the in-flight flag and lets the host re-place rather
// than leaving the URL stuck; the richer Retryable reschedule is doc 13's
// follow-up.
func (e *Engine) fold(res Result, inflight *int) {
	*inflight--
	now := e.clk.Now()
	if res.Err != nil {
		e.failed.Add(1)
		e.fr.Report(meguri.Outcome{URLKey: res.Req.URLKey, FetchedAt: now, Retryable: true}, now)
		return
	}
	e.fetched.Add(1)
	e.fr.Report(res.Outcome, now)
}

// intake drains the inbound transport for discoveries routed to this partition
// and folds each into the frontier, the receiver half of distribution (doc 12,
// section 6). It reports whether any new schedulable URL entered, so the run loop
// knows whether to re-dispatch instead of waiting. A single-partition engine has
// no router and intake is a no-op.
func (e *Engine) intake(now uint32) bool {
	if e.router == nil {
		return false
	}
	added := false
	for _, d := range e.router.Drain() {
		if e.fr.Discover(d, now) {
			added = true
		}
	}
	return added
}

// blockForInbound waits for the next inbound discovery to arrive when a live
// partition has drained its own frontier. It polls the transport against the
// context, returning true when work landed and false when the context is done. A
// router-less engine never reaches here, because UntilEmpty is the only sensible
// mode without inbound work.
func (e *Engine) blockForInbound(ctx context.Context) bool {
	if e.router == nil {
		return false
	}
	for {
		if e.intake(e.clk.Now()) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		default:
			e.clk.SleepUntil(ctx, e.clk.Now()+1)
		}
	}
}

// RouteSink adapts a router into the frontier's out-link sink (frontier.With
// LinkRouter): it ships the remote links to their owners and returns the local
// subset for this partition's intake. A transport send error is swallowed here on
// purpose, because the discovery transport is at-least-once and a dropped ship is
// rediscoverable; the link reappears the next time its source is crawled, and the
// receiver's seen-set dedups it. Pass it as
// frontier.WithLinkRouter(engine.RouteSink(router)).
func RouteSink(r *distribute.Router) func([]meguri.Discovery) []meguri.Discovery {
	return func(links []meguri.Discovery) []meguri.Discovery {
		local, _ := r.RouteLinks(links)
		return local
	}
}
