package meguri

import "strconv"

// URLStatus is the state-machine state of a frontier entry. The values are
// stable and stored in the status column (doc 03, doc 10).
type URLStatus uint8

const (
	StatusDiscovered     URLStatus = iota // seen, not yet scheduled
	StatusScheduled                       // queued for a first crawl
	StatusReady                           // eligible to dispatch now
	StatusInFlight                        // dispatched, awaiting an outcome
	StatusCrawled                         // fetched at least once
	StatusDueRecrawl                      // crawled, due for a refresh
	StatusGone                            // 410/404 stable, tombstoned
	StatusExcludedRobots                  // disallowed by robots.txt
	StatusTrapped                         // flagged by trap detection
)

// String names the status for human-readable reports (meguri stats, inspect).
// An out-of-range value prints its number so a forward-compatible reader never
// loses information.
func (s URLStatus) String() string {
	switch s {
	case StatusDiscovered:
		return "discovered"
	case StatusScheduled:
		return "scheduled"
	case StatusReady:
		return "ready"
	case StatusInFlight:
		return "in_flight"
	case StatusCrawled:
		return "crawled"
	case StatusDueRecrawl:
		return "due_recrawl"
	case StatusGone:
		return "gone"
	case StatusExcludedRobots:
		return "excluded_robots"
	case StatusTrapped:
		return "trapped"
	default:
		return "status(" + strconv.Itoa(int(s)) + ")"
	}
}

// DiscoverySource records how a URL entered the frontier.
type DiscoverySource uint8

const (
	SourceSeed     DiscoverySource = iota // from a seed list
	SourceLink                            // extracted from a crawled page
	SourceSitemap                         // listed in a sitemap
	SourceRedirect                        // a redirect target
	SourceManual                          // injected by hand
)

// URLRecord is the durable per-URL crawl state, one row per frontier entry,
// keyed by URLKey and sorted by URLKey so a host's rows are contiguous. The
// field names are stable; the frontier, freshness, dedup, prioritization,
// format, and store packages all reference them. A checkpoint serializes this
// straight into the URL table columns with no remapping (D1, D12).
type URLRecord struct {
	URLKey  URLKey // 128 bits, HostKey || PathKey
	HostKey uint64 // high half of URLKey, kept for host joins

	Status          URLStatus       // state-machine state
	Priority        float32         // OPIC + optional imported PageRank
	Depth           uint16          // link distance from nearest seed
	DiscoverySource DiscoverySource // seed | link | sitemap | redirect | manual

	URLRef uint64 // offset into the string region for the canonical URL

	FirstSeen   uint32 // epoch-hours, first discovery
	LastCrawled uint32 // epoch-hours, last successful fetch
	LastChanged uint32 // epoch-hours, last observed content change
	NextDue     uint32 // epoch-hours, next scheduled crawl

	Lambda         float32 // Poisson change rate, changes/hour
	CrawlCount     uint32  // successful fetches
	ChangeCount    uint32  // fetches that saw a change
	NoChangeStreak uint16  // consecutive no-change fetches

	ETagRef      uint64 // offset into string region, 0 if no ETag
	LastModified uint32 // epoch-hours, 0 if no Last-Modified

	ContentFP uint64 // content fingerprint of last body
	Simhash   uint64 // near-dup signature of last body

	HTTPStatus  uint16 // status of last crawl
	RedirectRef uint64 // reference to redirect-target record, 0 if none

	RetryCount uint8  // consecutive transient failures
	ErrorCount uint16 // lifetime failed fetches
}

// HostGrouping is the unit a HostKey groups by: the registrable domain (the
// default, PSL+1) or the full host.
type HostGrouping uint8

const (
	GroupRegistrableDomain HostGrouping = iota // default: PSL+1
	GroupFullHost                              // each fully-qualified host stands alone
)

// Host flag bits, stored in HostRecord.Flags.
const (
	HostFlagTrapSuspect uint16 = 1 << iota
	HostFlagRobotsMissing
	HostFlagDeadHost
	HostFlagGroupingOverride
)

// HostRecord is the durable per-host state, one row per host group, keyed and
// sorted by HostKey. It is the politeness, DNS, and robots state a URL's
// dispatch reads, plus the per-host budgets and imported quality signal. There
// are far fewer hosts than URLs, so the host table stays resident while the URL
// table stays mostly on disk (doc 03).
type HostRecord struct {
	HostKey uint64 // the host group identity

	HostRef        uint64       // offset into string region, the host group key
	Grouping       HostGrouping // registrable-domain or full-host
	RegistrableRef uint64       // offset into string region, the registrable domain

	ResolvedIP [16]byte // cached DNS result, IPv4-mapped into 16 bytes
	IPExpiry   uint32   // epoch-hours, DNS cache expiry

	RobotsFetched uint32 // epoch-hours, robots.txt last fetched
	RobotsExpiry  uint32 // epoch-hours, robots.txt cache expiry
	RobotsRef     uint64 // offset into blob region, parsed robots rules
	CrawlDelay    uint16 // deciseconds, effective crawl delay

	HostNextEligible uint32 // epoch-seconds, per-host token bucket
	IPNextEligible   uint32 // epoch-seconds, per-IP token bucket

	URLBudget uint32 // per-host URL cap
	URLCount  uint32 // current URLs of this host in the frontier
	DepthCap  uint16 // max crawl depth for this host

	HostScore float32 // imported host quality / PageRank, 0 if none

	CrawlTotal uint32 // lifetime successful fetches
	ErrorTotal uint32 // lifetime failed fetches
	AvgLatency uint16 // milliseconds, smoothed fetch latency

	Flags uint16 // trap-suspect | robots-missing | dead-host | grouping-override
}
