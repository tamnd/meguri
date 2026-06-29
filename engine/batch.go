package engine

import (
	"context"
	"sync"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
)

// Result is one finished fetch: the request that produced it, the outcome, and a
// non-nil Err only when the fetcher could not produce a usable outcome at all (a
// dead connection, not an HTTP error status, which is a normal outcome). The
// engine folds a Result back into the frontier on its single run goroutine.
type Result struct {
	Req     fetch.Request
	Outcome meguri.Outcome
	Err     error
}

// BatchFetcher wraps one concurrency-safe fetch.Fetcher in a bounded worker pool,
// the shape doc 13 specifies: requests go in, results come out in completion
// order, not request order, so a slow host never holds a fast one behind it. The
// pool size is the engine's polite-host parallelism, which is the only thing that
// bounds the number of fetches in flight (politeness, not the fetcher).
type BatchFetcher struct {
	f       fetch.Fetcher
	workers int
}

// NewBatchFetcher wraps f in a pool of the given size. A size below one is raised
// to one so the pool always makes progress.
func NewBatchFetcher(f fetch.Fetcher, workers int) *BatchFetcher {
	if workers < 1 {
		workers = 1
	}
	return &BatchFetcher{f: f, workers: workers}
}

// Workers reports the pool size, the in-flight bound the engine dispatches up to.
func (b *BatchFetcher) Workers() int { return b.workers }

// stream starts the worker pool and returns a send channel for requests and a
// receive channel for results in completion order. The request channel is
// unbuffered, so a send completes only when a worker is free, which is the
// backpressure that keeps in-flight fetches at the pool size. Closing the request
// channel drains the workers; the result channel closes once every worker exits.
func (b *BatchFetcher) stream(ctx context.Context) (chan<- fetch.Request, <-chan Result) {
	in := make(chan fetch.Request)
	out := make(chan Result, b.workers)
	var wg sync.WaitGroup
	wg.Add(b.workers)
	for range b.workers {
		go func() {
			defer wg.Done()
			for req := range in {
				o, err := b.f.Fetch(ctx, req)
				select {
				case out <- Result{Req: req, Outcome: o, Err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()
	return in, out
}

// FetchBatch is the doc-13 literal surface: it fetches a fixed slice of requests
// through the pool and returns a channel of their outcomes in completion order,
// closed when the last one lands. A fetch that errors is dropped from the stream
// rather than surfaced, because FetchBatch is the convenience wrapper for callers
// that only want outcomes; the engine uses the lower-level stream so it sees the
// errors and can re-place a failed host.
func (b *BatchFetcher) FetchBatch(ctx context.Context, reqs []fetch.Request) <-chan meguri.Outcome {
	out := make(chan meguri.Outcome, len(reqs))
	if len(reqs) == 0 {
		close(out)
		return out
	}
	in, results := b.stream(ctx)
	go func() {
		for _, r := range reqs {
			select {
			case in <- r:
			case <-ctx.Done():
				close(in)
				return
			}
		}
		close(in)
	}()
	go func() {
		defer close(out)
		for range reqs {
			res, ok := <-results
			if !ok {
				return
			}
			if res.Err != nil {
				continue
			}
			out <- res.Outcome
		}
	}()
	return out
}
