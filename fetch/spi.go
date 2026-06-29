// Package fetch defines the boundary between the frontier engine and whatever
// actually retrieves bytes from the network. The engine never opens a socket:
// it hands a Fetcher a Request built from a URL's and its host's durable state,
// and consumes the meguri.Outcome the Fetcher returns. ami (網) is the
// production Fetcher; tests use a recorded-corpus Fetcher that replays real
// ccrawl responses, so the scheduler is exercised against real headers, real
// redirects, and real not-modified responses without touching the network.
package fetch

import (
	"context"

	"github.com/tamnd/meguri"
)

// Request is one unit of work the engine dispatches: a single URL to fetch,
// carried with the host state the fetcher needs to be polite and conditional.
// It is built from a meguri.URLRecord and its meguri.HostRecord at dispatch
// time; the fetcher does not read the frontier.
type Request struct {
	URLKey       meguri.URLKey // identity, echoed back on the outcome
	HostKey      uint64        // politeness and routing key
	CanonicalURL string        // the URL to fetch

	// Conditional-GET inputs, empty or zero when the URL has never been
	// fetched. A fetcher that honors them turns an unchanged page into a cheap
	// 304, which the freshness model reads as a no-change observation.
	ETag         string // last ETag, for If-None-Match
	LastModified uint32 // last Last-Modified epoch-hours, for If-Modified-Since

	// ResolvedIP is the host's cached DNS answer, IPv4-mapped into 16 bytes,
	// zero when the fetcher must resolve. Passing it lets the engine pin a
	// connection to the IP the per-IP politeness bucket accounted for.
	ResolvedIP [16]byte

	// Depth is the URL's own link distance from the nearest seed, carried so a
	// fetcher that extracts out-links can stamp each candidate's depth as this
	// plus one without consulting the frontier. The frontier fills it from the
	// dispatched record; a seed dispatches at depth zero.
	Depth uint16

	// Robots is set when this request is the host's robots.txt, fetched before
	// any of its content URLs (doc 07). CanonicalURL is the robots.txt URL; the
	// fetcher returns the raw body in Outcome.RobotsBody for meguri to parse.
	Robots bool
}

// Fetcher retrieves one Request and returns its typed Outcome. An error is for
// the transport failing in a way that yields no usable outcome at all; an HTTP
// error status is a normal Outcome with that status, not a Go error. A Fetcher
// must be safe for concurrent use: the engine dispatches many requests at once,
// bounded by politeness, not by the fetcher.
type Fetcher interface {
	Fetch(ctx context.Context, req Request) (meguri.Outcome, error)
}
