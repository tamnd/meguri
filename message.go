package meguri

// AnchorHint is a coarse anchor-text quality bucket carried on a discovery so
// the prioritizer and the spam guards read it without the full anchor string.
type AnchorHint uint8

const (
	AnchorUnknown     AnchorHint = iota // not computed
	AnchorEmpty                         // no anchor text
	AnchorGeneric                       // "click here", "read more"
	AnchorDescriptive                   // real descriptive text
	AnchorSpammy                        // matches a spam-anchor pattern
)

// Discovery is the one-way idempotent message a partition sends when it finds a
// link for a host it does not own (D16). It is not durable. Idempotency comes
// from the receiver's seen-set, not from any field here: the same discovery
// delivered twice deduplicates to one frontier entry, so the transport may be
// at-least-once.
type Discovery struct {
	URLKey URLKey // identity and routing key

	CanonicalURL string // the canonical URL text, inline (crosses partitions)

	Depth           uint16          // source depth + 1, candidate depth
	DiscoverySource DiscoverySource // seed | link | sitemap | redirect | manual

	SrcHostKey uint64     // HostKey of the page the link was found on
	LinkWeight float32    // OPIC cash this link carries from the source
	AnchorHint AnchorHint // coarse anchor-text quality bucket

	ObservedAt uint32 // epoch-hours, when discovered
}

// Outcome is the typed result of one fetch, the first-class value that closes
// the loop (D18). It is not durable: the fetcher returns it through the SPI and
// the engine consumes it to update the URL's state, its change-rate estimate,
// its next-due time, its host's adaptive rate, and the importance signals.
//
// A fetch is a request-outcome pair: the engine dispatches a URL and the
// fetcher returns exactly one Outcome. Freshness reads NotModified, ContentFP,
// Simhash, and FetchedAt; politeness reads LatencyMS, HTTPStatus, and
// RobotsCrawlDelay; prioritization reads Links and their per-link cash.
type Outcome struct {
	URLKey URLKey // which URL this outcome is for

	HTTPStatus uint16 // the status returned
	FetchedAt  uint32 // epoch-hours, fetch completion
	LatencyMS  uint16 // round-trip latency, milliseconds

	NotModified bool   // 304 from the conditional GET
	ContentFP   uint64 // fingerprint of the body, 0 if none
	Simhash     uint64 // near-dup signature, 0 if none
	ContentLen  uint32 // body length in bytes

	ETag           string // ETag header, empty if none
	LastModified   uint32 // epoch-hours, 0 if none
	RedirectTarget string // resolved redirect URL, empty if none

	Retryable        bool   // transient failure, retry warranted
	RetryAfter       uint16 // deciseconds from a Retry-After header, 0 if none
	RobotsCrawlDelay uint16 // deciseconds, fresh Crawl-delay if robots re-read

	// RobotsBody is the raw robots.txt bytes, populated only for a robots fetch
	// (fetch.Request.Robots). Parsing is policy and stays in meguri (doc 07): the
	// fetcher hands back the bytes, the frontier turns them into rules.
	RobotsBody []byte

	Links []Discovery // extracted out-links, canonicalized and ready to route
}
