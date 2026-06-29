// Package politeness enforces the rules that keep a crawl from hammering a host:
// the per-host and per-IP token buckets, the crawl-delay derived from robots.txt
// and adaptive backoff, and the gate that decides when a host's next fetch is
// eligible. It reads and writes the politeness fields of meguri.HostRecord and
// is consulted by the frontier scheduler before every dispatch.
//
// This is part of the M3 milestone (politeness, DNS, robots, conditional GET).
// The package is a placeholder until then.
package politeness
