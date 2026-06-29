// Package dns is the host-resolution cache: it maps a host to its resolved IP,
// honors TTLs, and feeds the per-IP politeness bucket so two hosts behind one
// address do not dodge rate limits. It maintains the ResolvedIP and IPExpiry
// fields of meguri.HostRecord and resolves lazily, just before a host first
// becomes eligible to crawl.
//
// This is part of the M3 milestone. The package is a placeholder until then.
package dns
