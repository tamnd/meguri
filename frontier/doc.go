// Package frontier is the in-partition crawl frontier: the bounded-memory
// Mercator-style queue that turns a partition's URL records into a stream of
// dispatch decisions in priority-then-politeness order. It owns the front banks
// (priority queues that decide what to crawl next) and the back banks (per-host
// FIFOs that enforce one in-flight fetch per host), the host heap keyed by each
// host's next-eligible time, and the scheduler loop that drains them against a
// stub or real fetcher.
//
// This is the M1 milestone. The package is a placeholder until then so the
// layout the spec pins exists from the first commit.
package frontier
