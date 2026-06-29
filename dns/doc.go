// Package dns is the host-resolution cache: it maps a host to its resolved IP,
// honors TTLs, and feeds the per-IP politeness bucket so two hosts behind one
// address do not dodge rate limits.
//
// The point of the package is to keep DNS off the dispatch hot path. Resolution
// is slow and bursty, so the dispatcher never blocks on a lookup: it asks the
// Cache for a host with Lookup, and on a miss it queues the host with Prefetch
// and moves on. A bounded worker pool resolves queued hosts in the background
// and fills the positive cache. A name that fails to resolve lands in the
// negative cache and is suppressed for a while so a dead host does not get
// retried on every dispatch.
//
// The real resolver runs over the pure-Go net resolver (PreferGo: true). The
// cgo resolver blocks an OS thread per lookup and exhausts the thread limit at
// crawl scale; the pure-Go resolver uses goroutines instead.
//
// DialContext rewrites a dial address to the cached IP so a connection goes
// straight to the resolved address while the hostname still rides in the Host
// header and TLS SNI. That keeps the per-IP politeness bucket keyed on exactly
// the IP we dialed.
package dns
