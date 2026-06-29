// Package frontier is meguri's scheduler: the decision layer that holds the set
// of URLs to crawl and answers, at every moment, which URL to fetch next. It
// sits between ami (網), which fetches bytes, and tsumugi (紡), which stores and
// ranks them, and it never touches the network itself. It hands a fetch.Fetcher
// a Request and consumes the meguri.Outcome that comes back (D18).
//
// This is the M1 engine: a single partition, fully resident, implementing the
// Mercator model of doc 05. URLs enter a front bank ordered by priority (D4). A
// distributor binds them to per-host back queues, one in flight per host. A
// host heap parks hosts whose politeness window is closed and a ready bank holds
// hosts that may dispatch now, ordered by their best URL. Dispatch therefore
// honors two rules at once: never fetch from a host inside its minimum interval,
// and among hosts that may be fetched, always prefer the higher-priority URL.
//
// The resident state is bounded by the number of active hosts, not by the size
// of the frontier: at most `target` hosts hold a back queue at any time, and the
// rest of the URLs wait in the front bank. A checkpoint serializes the whole
// engine into a .meguri partition (D1, D12) and a recovery rebuilds an identical
// scheduler, so the dispatch sequence survives a restart unchanged.
package frontier

import (
	"context"
	"sort"
	"time"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/dns"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/freshness"
	"github.com/tamnd/meguri/politeness"
	"github.com/tamnd/meguri/prioritize"
	"github.com/tamnd/meguri/robots"
)

// defaultTarget is the active-host cap when none is set. It is effectively
// unbounded for a single partition, so by default every host with work is
// active and dispatch honors global priority order exactly. Lower it with
// WithTarget to bound resident memory to a fixed number of back queues, the
// knob doc 05 ties to k*threads.
const defaultTarget = 1 << 30

// recrawlGapHours is the placeholder next-due interval M1 stamps on a crawled
// URL. Real freshness scheduling (the Poisson model of doc 06) lands in M4; for
// now a fetched URL simply parks a week out so it does not re-dispatch inside a
// run.
const recrawlGapHours = 168

// robots fetch state of a host: never fetched, fetch in flight, or rules ready.
const (
	robotsNone uint8 = iota
	robotsPending
	robotsReady
)

// hostEntry is the resident state of one host: its durable record, its back
// queue of URLs waiting to dispatch (FIFO, fed in priority order so the head is
// the host's best URL), and the live politeness, robots, and pool bookkeeping.
type hostEntry struct {
	rec      meguri.HostRecord
	back     []meguri.URLKey // FIFO; head is the next URL to dispatch for this host
	inFlight bool            // a URL of this host is dispatched, awaiting an outcome
	active   bool            // holds a back queue, counts against target

	// M3 politeness and robots state. The durable copy of the politeness window
	// lives in rec (HostNextEligible, IPNextEligible, CrawlDelay); these are the
	// in-memory control signals that drive it.
	effective   time.Duration // adaptive crawl interval, AIMD-controlled
	crawlFloor  time.Duration // configured/robots floor, never crawl faster
	robots      *robots.Rules // parsed rules, nil means allow-all
	robotsState uint8         // robotsNone | robotsPending | robotsReady
	ceilStreak  uint8         // consecutive ceiling-pinned error fetches
}

// Frontier is the single-partition resident scheduler.
type Frontier struct {
	id      uint32
	created uint32
	codec   uint8

	records map[meguri.URLKey]*meguri.URLRecord
	hosts   map[uint64]*hostEntry
	arena   arena
	seen    *dedup.SeenSet // M2 dedup authority, idempotent intake (doc 08, D5)
	soft    *dedup.SoftDetector

	urlFront   prioRing[meguri.URLKey] // URLs not yet bound to a host back queue
	readyHosts prioRing[uint64]        // hosts eligible now, keyed by best URL priority
	wait       waitHeap                // hosts parked until their politeness window opens

	target int // active-host cap (distributor)
	active int // hosts currently holding a back queue

	// M3 politeness, DNS, and robots policy.
	pol      politeness.Config
	ips      *politeness.IPTable
	resolver *dns.Cache // nil disables DNS prefetch and per-IP dial pinning
	robotsOn bool       // fetch and enforce robots.txt before content
	agent    string     // product token robots groups are matched against

	// M4 freshness rescheduler (doc 06). nil leaves the M1 placeholder next-due in
	// place so the earlier milestones' dispatch sequences are byte-for-byte
	// unchanged. When set, a crawled URL's lambda and next_due come from the
	// Poisson change-rate model and the water-filling allocation.
	fresh    *freshness.Params
	tau      *freshness.TauController // global water level, slowly retuned to the budget
	reschedN int                      // reschedules since the last tau tick

	// M5 prioritization (doc 09). nil leaves the seed/link priority untouched so
	// the earlier milestones' dispatch order is byte-for-byte unchanged. When set,
	// a seed endows OPIC cash, a discovery credits cash and cross-host reputation
	// and is admitted under the STAR budget, and a crawl distributes its cash
	// across its out-links, so the front bank orders by online importance.
	prio *prioritize.Prioritizer

	// linkSink, when set, splits a crawl's out-links into local and remote as the
	// cash spreads (doc 04, doc 12, section 6). It receives every out-link after
	// the OPIC cash split has stamped each one's LinkWeight, ships the remote ones
	// to their owning partitions, and returns the subset this partition owns for
	// the local intake. nil is the single-partition case where every link is local.
	// Because the sink sees the links only after the split, a remote link still
	// carries the cash its source granted, which its owner credits on receipt.
	linkSink func([]meguri.Discovery) []meguri.Discovery

	// stateOn turns on the full outcome state machine (doc 13, the state update).
	// nil/false keeps the earlier milestones' behavior: every outcome marks the URL
	// Crawled and folds into AIMD once, so the M3 corpus gate (a 5xx counts as one
	// folded error, not a retried one) is byte-for-byte unchanged. When set, a
	// transient failure backs off and re-queues up to a retry limit then tombstones,
	// a 404/410 tombstones, and a redirect creates the target record and points the
	// source at it.
	stateOn bool
}

// tauTickEvery is how many reschedules pass between background re-estimates of
// the water level. tau moves slowly (doc 06, section 8), so a re-tune every few
// thousand crawls tracks the drift without putting an O(N) sweep on the hot path.
const tauTickEvery = 4096

// Option configures a Frontier at construction.
type Option func(*Frontier)

// WithTarget caps the number of hosts that hold a back queue at once, bounding
// resident memory. A value <= 0 is ignored. The default is effectively
// unbounded.
func WithTarget(n int) Option {
	return func(f *Frontier) {
		if n > 0 {
			f.target = n
		}
	}
}

// WithPoliteness sets the politeness policy (interval band and AIMD constants)
// and rebuilds the per-IP table around its IP floor. The default is
// politeness.DefaultConfig (doc 07).
func WithPoliteness(c politeness.Config) Option {
	return func(f *Frontier) {
		f.pol = c
		f.ips = politeness.NewIPTable(c.IPFloor)
	}
}

// WithResolver turns on DNS: hosts are prefetched off the dispatch path, their
// resolved IP rides on each fetch.Request so ami can pin the connection, and the
// per-IP politeness bucket is keyed on the address many vhosts may share. Without
// it the frontier crawls host-only, with no per-IP throttle.
func WithResolver(r dns.Resolver) Option {
	return func(f *Frontier) {
		f.resolver = dns.NewCache(r, nil)
	}
}

// WithRobots turns on robots.txt: a host fetches and parses robots before any of
// its content URLs dispatch, disallowed URLs are excluded, and a robots
// Crawl-delay raises the host's politeness floor. agent is the product token its
// groups are matched against. Without it the frontier does not consult robots.
func WithRobots(agent string) Option {
	return func(f *Frontier) {
		f.robotsOn = true
		if agent != "" {
			f.agent = agent
		}
	}
}

// WithFreshness turns on the Poisson rescheduler (doc 06): a crawled URL's change
// rate is estimated from its history, its recrawl interval is set by the
// water-filling allocation against a global water level retuned toward
// budgetPerHour refresh crawls per hour, and its next_due is spaced
// deterministically. Without it a crawled URL parks a flat week out, the M1
// placeholder. A budgetPerHour <= 0 disables the budget pressure, leaving every
// URL funded to its own optimal rate under the per-host politeness cap.
func WithFreshness(p freshness.Params, budgetPerHour float64) Option {
	return func(f *Frontier) {
		pp := p
		f.fresh = &pp
		f.tau = freshness.NewTauController(budgetPerHour)
	}
}

// WithPrioritizer turns on OPIC importance ordering (doc 09): a seed endows its
// cash, every discovered out-link credits its OPIC cash and cross-host
// reputation to its target, the target host's crawl budget tracks the distinct
// other domains that link to it (STAR), and a crawl distributes the source's cash
// across its out-links so the front bank orders by online importance refined as
// the crawl runs. Imported PageRank or host quality from a prior tsumugi crawl is
// blended in through the Prioritizer when present. Without it a URL keeps the
// flat seed or link-weight priority of the earlier milestones.
func WithPrioritizer(p prioritize.Params) Option {
	return func(f *Frontier) {
		f.prio = prioritize.New(p)
	}
}

// WithLinkRouter routes a crawl's out-links to their owning partitions (doc 04,
// doc 12). sink receives every out-link after the OPIC cash split, ships the
// remote ones to their owners, and returns the subset this partition owns for the
// local intake. Without it every out-link is treated as local, the
// single-partition behavior. The engine wires the router's RouteLinks behind this
// so the fold splits local from remote in one place.
func WithLinkRouter(sink func([]meguri.Discovery) []meguri.Discovery) Option {
	return func(f *Frontier) { f.linkSink = sink }
}

// WithStateMachine turns on the full outcome state machine (doc 13, the state
// update). Without it every outcome marks the URL Crawled, the earlier
// milestones' behavior. With it the outcome drives the URL through its
// transitions: a 200 or 304 to Crawled, a transient failure (Retryable, or a
// 429/5xx) backed off and re-queued up to a retry limit then to Gone, a 404 or
// 410 to Gone, and a redirect to Crawled with the target canonicalized, created
// as its own record, and the source's redirect_ref pointed at it. The engine
// turns this on for a live crawl; the scheduler-only gates leave it off so their
// dispatch counts stay exact.
func WithStateMachine() Option {
	return func(f *Frontier) { f.stateOn = true }
}

// New returns an empty frontier for partition id, stamped created (epoch-hours)
// as its build time.
func New(id, created uint32, opts ...Option) *Frontier {
	f := &Frontier{
		id:      id,
		created: created,
		codec:   format.CodecZstd,
		records: make(map[meguri.URLKey]*meguri.URLRecord),
		hosts:   make(map[uint64]*hostEntry),
		arena:   newArena(),
		seen:    dedup.NewSeenSet(),
		soft:    dedup.NewSoftDetector(),
		target:  defaultTarget,
		pol:     politeness.DefaultConfig(),
		agent:   "meguri",
	}
	f.ips = politeness.NewIPTable(f.pol.IPFloor)
	for _, o := range opts {
		o(f)
	}
	return f
}

// Len reports the number of URLs the frontier holds, crawled or not.
func (f *Frontier) Len() int { return len(f.records) }

// Pending reports the number of URLs still waiting to be dispatched, in the
// front bank or a back queue.
func (f *Frontier) Pending() int {
	n := f.urlFront.len()
	for _, h := range f.hosts {
		n += len(h.back)
	}
	return n
}

// Seed inserts a first-crawl candidate. url is the canonical URL, host its
// grouping string, priority its initial importance, firstSeen and nextDue
// epoch-hours, and crawlDelay the host's politeness interval in deciseconds. A
// URL already present is ignored, so seeding is idempotent. M1 treats every
// seed as immediately schedulable; the due-time index that would defer a
// not-yet-due URL is the timing wheel of M6.
func (f *Frontier) Seed(url, host string, priority float32, firstSeen, nextDue uint32, crawlDelay uint16) {
	hk := meguri.HostKeyOf(host)
	key := meguri.URLKey{HostKey: hk, PathKey: meguri.PathKeyOf(PathOf(url))}
	// The seen-set is the dedup authority (doc 08, D5): a key already seen is a
	// rediscovery and seeding it again is a no-op, so seeding is idempotent.
	if f.seen.Seen(key) {
		return
	}
	rec := &meguri.URLRecord{
		URLKey:          key,
		HostKey:         hk,
		Status:          meguri.StatusScheduled,
		Priority:        priority,
		URLRef:          f.arena.intern(url),
		FirstSeen:       firstSeen,
		NextDue:         nextDue,
		DiscoverySource: meguri.SourceSeed,
	}
	f.records[key] = rec

	h := f.hosts[hk]
	if h == nil {
		h = f.newHost(hk, f.arena.intern(host), host, crawlDelay)
		f.hosts[hk] = h
	}
	h.rec.URLCount++

	// With prioritization on, the seed's importance becomes its OPIC cash
	// endowment, the starting cash the first crawl distributes, and the front-bank
	// priority is the blended, penalized score (doc 09). Without it the seed keeps
	// the caller's flat priority, the earlier-milestone behavior.
	if f.prio != nil {
		f.prio.SeedCash(key, priority)
		rec.Priority = f.prio.Priority(rec, &h.rec)
	}
	f.urlFront.push(key, rec.Priority)
}

// Discover is the idempotent intake of a routed out-link (doc 08, section 9.3,
// onDiscovery). It is the M2 closed-loop entry the link extractor feeds: a
// Discovery carries a canonical URLKey, its depth from the seed, and the OPIC
// cash the source link grants. Discover deduplicates the key against the seen-set
// and, for a genuinely new URL, applies the blunt trap defense (doc 08, section
// 8.2): a discovery too deep or over the host's budget is parked in Trapped
// rather than scheduled, so the row exists and dedups rediscoveries but does not
// consume crawl budget. It returns true when a new schedulable URL entered the
// frontier.
//
// Delivering the same discovery twice creates at most one record, which is what
// lets the discovery transport be at-least-once (D16).
func (f *Frontier) Discover(d meguri.Discovery, now uint32) bool {
	if f.seen.Seen(d.URLKey) {
		// Rediscovery: the row already exists. With prioritization on, the link
		// still carries OPIC cash and cross-host reputation, so credit it and
		// reprice the target (doc 09); without it M2's contract holds and only the
		// dedup matters.
		if f.prio != nil {
			if rec := f.records[d.URLKey]; rec != nil {
				f.creditDiscovery(d, rec)
			}
		}
		return false
	}

	hk := d.URLKey.HostKey
	h := f.hosts[hk]
	if h == nil {
		host := hostFromCanonical(d.CanonicalURL)
		h = f.newHost(hk, f.arena.intern(host), host, 10)
		f.hosts[hk] = h
	}

	// Credit the link's cash and reputation before admitting, so a host that just
	// earned a distinct cross-host in-link is budgeted on its new reputation
	// (doc 09, STAR) and the URL enters at its blended importance.
	priority := d.LinkWeight
	if f.prio != nil {
		indeg := f.prio.Credit(d)
		prioritize.UpdateHostBudget(&h.rec, indeg, f.prio.Params())
	}

	status := dedup.Admit(d.Depth, &h.rec, true)
	rec := &meguri.URLRecord{
		URLKey:          d.URLKey,
		HostKey:         hk,
		Status:          status,
		Priority:        priority,
		Depth:           d.Depth,
		URLRef:          f.arena.intern(d.CanonicalURL),
		FirstSeen:       d.ObservedAt,
		DiscoverySource: d.DiscoverySource,
	}
	if f.prio != nil {
		rec.Priority = f.prio.Priority(rec, &h.rec)
	}
	f.records[d.URLKey] = rec
	h.rec.URLCount++

	if status != meguri.StatusScheduled {
		return false // parked in Trapped: recorded, dedups, not queued
	}
	f.urlFront.push(d.URLKey, rec.Priority)
	return true
}

// Warm pre-populates a never-crawled URL with the validators a prior crawl left,
// so its first fetch this campaign goes straight to a conditional GET (doc 13's
// seed pre-population). It stamps the ETag, the Last-Modified epoch-hours, and the
// prior content fingerprint a seed list carried from an earlier crawl. It is a
// no-op on a missing record or one already crawled this campaign, so re-warming a
// live URL never rewrites fresher state. The prior fingerprint seeds the
// change-rate comparison, so the first refetch is classified as change or
// no-change rather than treated as a first sighting.
func (f *Frontier) Warm(key meguri.URLKey, etag string, lastModified uint32, prevDigest uint64) {
	rec := f.records[key]
	if rec == nil || rec.CrawlCount > 0 {
		return
	}
	if etag != "" {
		rec.ETagRef = f.arena.intern(etag)
	}
	if lastModified != 0 {
		rec.LastModified = lastModified
	}
	if prevDigest != 0 {
		rec.ContentFP = prevDigest
	}
}

// Dispatch returns the next URL to fetch at clock time now (epoch-seconds), or
// ok=false when no host may be fetched at now. A false result does not mean the
// frontier is drained: call NextEligible to learn whether advancing the clock
// would open a host. The caller fetches the returned Request and feeds the
// outcome back through Report.
func (f *Frontier) Dispatch(now uint32) (fetch.Request, bool) {
	f.promote(now)
	f.distribute(now)

	// Pop the best ready host, but resolve its address and re-check the live
	// window first: a sibling host sharing the same IP may have advanced the
	// per-IP bucket since this host was marked ready. If the IP now gates it,
	// re-park it and try the next ready host (doc 07, both buckets must permit).
	var h *hostEntry
	for {
		hk, ok := f.readyHosts.pop()
		if !ok {
			return fetch.Request{}, false
		}
		h = f.hosts[hk]
		f.resolveHost(h, now)
		if e := f.eligibleNow(h); e > now {
			f.wait.push(hk, e)
			continue
		}
		break
	}

	// Robots first: a host with content work but no fresh robots rules fetches
	// robots.txt before any of its content URLs (doc 07). The robots fetch spends
	// politeness like any other request to the host.
	if f.robotsOn && f.needsRobots(h, now) {
		h.robotsState = robotsPending
		h.inFlight = true
		f.spend(h, now)
		return f.robotsRequest(h), true
	}

	key := h.back[0]
	rec := f.records[key]
	rec.Status = meguri.StatusInFlight
	h.inFlight = true
	f.spend(h, now)
	return fetch.Request{
		URLKey:       rec.URLKey,
		HostKey:      rec.HostKey,
		CanonicalURL: f.arena.str(rec.URLRef),
		ETag:         f.arena.str(rec.ETagRef),
		LastModified: rec.LastModified,
		ResolvedIP:   h.rec.ResolvedIP,
	}, true
}

// NextEligible returns the earliest epoch-seconds at which some parked host
// becomes eligible, or ok=false when no host is waiting (the frontier is
// drained for this run). When Dispatch returns false, the scheduler advances
// its clock to this time and tries again.
func (f *Frontier) NextEligible() (uint32, bool) {
	it, ok := f.wait.peekMin()
	if !ok {
		return 0, false
	}
	return it.eligible, true
}

// Report records the outcome of a dispatched URL at clock time now. It marks the
// URL crawled, clears the host's in-flight flag, and re-places the host: back in
// the wait heap behind its fresh politeness window if it still has work, or out
// of the active set if its back queue drained.
func (f *Frontier) Report(o meguri.Outcome, now uint32) {
	// A robots.txt outcome has no URL record: it carries the host's robots key
	// and the raw body. Parse it, cache the rules, and let the host proceed to
	// its content URLs (doc 07).
	if f.robotsOn {
		if h := f.hosts[o.URLKey.HostKey]; h != nil && h.robotsState == robotsPending && o.URLKey == robotsKey(h.rec.HostKey) {
			f.applyRobots(h, o, now)
			return
		}
	}

	rec := f.records[o.URLKey]
	if rec == nil {
		return
	}
	h := f.hosts[rec.HostKey]
	if h != nil && len(h.back) > 0 && h.back[0] == o.URLKey {
		h.back = h.back[1:]
	}
	rec.HTTPStatus = o.HTTPStatus

	// Drive the URL through its outcome state machine (doc 13, the state update).
	// With the state machine off, every outcome is a crawl: the earlier milestones
	// fold a 5xx into AIMD and call it done, which is what their gates assert. With
	// it on, the outcome's status and Retryable flag pick the transition.
	if f.stateOn {
		switch {
		case o.NotModified:
			f.markCrawled(rec, h, o, now) // 304: a no-change crawl
		case o.RedirectTarget != "" || (o.HTTPStatus >= 300 && o.HTTPStatus < 400):
			f.recordRedirect(rec, o, now) // create the target, point the source at it
			f.markCrawled(rec, h, o, now) // the source resolved
		case o.Retryable || (o.HTTPStatus >= 400 && o.HTTPStatus <= 599):
			f.failURL(rec, h, o) // transient backoff-and-retry, or 404/410 to Gone
		default:
			f.markCrawled(rec, h, o, now) // 2xx
		}
	} else {
		f.markCrawled(rec, h, o, now)
	}

	if h == nil {
		return
	}
	// Fold the outcome into the host's adaptive rate before re-placing it (doc 07).
	f.adapt(h, o)
	h.inFlight = false
	if len(h.back) == 0 {
		h.active = false
		f.active--
		return
	}
	f.place(h, now)
}

// promote drains the wait heap of every host whose politeness window has opened
// by now, moving each into the ready bank keyed by its best URL's priority.
func (f *Frontier) promote(now uint32) {
	for {
		it, ok := f.wait.peekMin()
		if !ok || it.eligible > now {
			return
		}
		f.wait.popMin()
		h := f.hosts[it.hostKey]
		if h == nil || len(h.back) == 0 {
			continue
		}
		// Re-check the live window: another host on the same IP may have advanced
		// the per-IP bucket after this host was parked, so the heap key can be
		// stale. If it is, re-park behind the fresh instant rather than dispatch
		// early.
		if e := f.eligibleNow(h); e > now {
			f.wait.push(it.hostKey, e)
			continue
		}
		f.readyHosts.push(it.hostKey, f.records[h.back[0]].Priority)
	}
}

// distribute binds front-bank URLs to host back queues, activating new hosts up
// to the target. It pulls the highest-priority URL whose host can take it: an
// already-active host always can, an idle host only when there is room for
// another active host. Pulling stops at the first URL that cannot be placed, so
// the highest-priority work is always bound first.
func (f *Frontier) distribute(now uint32) {
	for {
		key, ok := f.urlFront.peek()
		if !ok {
			return
		}
		h := f.hosts[key.HostKey]
		if !h.active && f.active >= f.target {
			return
		}
		f.urlFront.pop()
		// A host whose robots rules are already known excludes a disallowed URL at
		// bind time rather than queueing it (doc 07). A host still awaiting robots
		// queues the URL and filters it when the rules land.
		if h.robotsState == robotsReady && !f.allowed(h, key) {
			f.records[key].Status = meguri.StatusExcludedRobots
			continue
		}
		wasActive := h.active
		h.back = append(h.back, key)
		if !wasActive {
			h.active = true
			f.active++
			f.place(h, now)
		}
	}
}

// place files an active host into the pool its state calls for: nowhere if it
// has no work or is in flight, the ready bank if its window is open, the wait
// heap if it must still cool down.
func (f *Frontier) place(h *hostEntry, now uint32) {
	if len(h.back) == 0 || h.inFlight {
		return
	}
	e := f.eligibleNow(h)
	if e <= now {
		f.readyHosts.push(h.rec.HostKey, f.records[h.back[0]].Priority)
		return
	}
	f.wait.push(h.rec.HostKey, e)
}

// Drain dispatches every schedulable URL, advancing a logical clock from start
// over politeness waits, and returns the dispatch order. It is the synchronous
// driver the M1 gate runs: fetch through fr, feed each outcome straight back,
// and record what went out in what order. A real engine dispatches many hosts
// at once; Drain serializes them so the ordering guarantees are checkable.
func (f *Frontier) Drain(ctx context.Context, start uint32, fr fetch.Fetcher) ([]Dispatched, error) {
	var out []Dispatched
	now := start
	for {
		req, ok := f.Dispatch(now)
		if ok {
			o, err := fr.Fetch(ctx, req)
			if err != nil {
				return out, err
			}
			out = append(out, Dispatched{Key: req.URLKey, HostKey: req.HostKey, At: now})
			f.Report(o, now)
			continue
		}
		t, ok := f.NextEligible()
		if !ok || t <= now {
			return out, nil
		}
		now = t
	}
}

// Dispatched is one entry of a dispatch stream: which URL went out, for which
// host, at what clock time.
type Dispatched struct {
	Key     meguri.URLKey
	HostKey uint64
	At      uint32
}

// Checkpoint serializes the live frontier into a .meguri partition (D1, D12):
// every URL record sorted by URLKey, every host record sorted by HostKey with
// its live politeness window (already maintained in the record), and the string
// arena. Recover rebuilds an identical scheduler from it.
func (f *Frontier) Checkpoint() *format.Partition {
	urls := make([]meguri.URLRecord, 0, len(f.records))
	for _, r := range f.records {
		urls = append(urls, *r)
	}
	sort.Slice(urls, func(i, j int) bool { return urls[i].URLKey.Less(urls[j].URLKey) })

	hosts := make([]meguri.HostRecord, 0, len(f.hosts))
	for _, h := range f.hosts {
		hosts = append(hosts, h.rec)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostKey < hosts[j].HostKey })

	lo, hi := uint64(0), ^uint64(0)
	if len(hosts) > 0 {
		lo, hi = hosts[0].HostKey, hosts[len(hosts)-1].HostKey
	}
	return &format.Partition{
		ID:           f.id,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: f.created,
		DefaultCodec: f.codec,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      append([]byte(nil), f.arena.buf...),
	}
}

// CheckpointBytes checkpoints and encodes to the on-disk .meguri image.
func (f *Frontier) CheckpointBytes() ([]byte, error) {
	return format.Encode(f.Checkpoint())
}

// Recover rebuilds a frontier from a checkpoint partition. The resident pools
// are derived from the durable state: every host comes back with its politeness
// time, every uncrawled URL re-enters the front bank in URLKey order, and any
// URL caught in flight at the checkpoint resets to scheduled so it dispatches
// again. Rebuilding in URLKey order with the same deterministic tie-breaks
// reproduces the exact dispatch sequence the original would have continued.
func Recover(p *format.Partition, opts ...Option) *Frontier {
	f := New(p.ID, p.CreatedHours, opts...)
	f.codec = p.DefaultCodec
	f.arena = arena{buf: append([]byte(nil), p.Strings...)}

	for i := range p.Hosts {
		h := p.Hosts[i]
		// The adaptive interval is a transient control signal: it resets to the
		// baseline and re-converges, while the durable floor (CrawlDelay) and the
		// politeness window (HostNextEligible) come straight back from the record.
		f.hosts[h.HostKey] = &hostEntry{
			rec:        h,
			effective:  f.pol.Default,
			crawlFloor: deciToDur(h.CrawlDelay),
		}
	}
	for i := range p.URLs {
		rec := p.URLs[i]
		if rec.Status == meguri.StatusInFlight {
			rec.Status = meguri.StatusScheduled
		}
		r := rec
		f.records[r.URLKey] = &r
		// Rebuild the seen-set from the durable key column (doc 08, section 5.3:
		// the live ribbon is rebuilt from the urlkey column on reload), so a
		// post-recovery discovery dedups against everything the partition holds.
		f.seen.Insert(r.URLKey)
		switch r.Status {
		case meguri.StatusScheduled, meguri.StatusReady, meguri.StatusDueRecrawl:
			if f.hosts[r.HostKey] == nil {
				host := HostOf(f.arena.str(r.URLRef))
				f.hosts[r.HostKey] = f.newHost(r.HostKey, f.arena.intern(host), host, 10)
			}
			f.urlFront.push(r.URLKey, r.Priority)
		}
	}
	return f
}

// hostFromCanonical returns the registrable-domain group key for a canonical URL,
// the string the host record's HostRef points at. It falls back to the raw host
// split when the URL is not parseable as canonical, so a malformed discovery
// still names a host.
func hostFromCanonical(canon string) string {
	host := HostOf(canon)
	if rd := dedup.RegistrableDomain(host); rd != "" {
		return rd
	}
	return host
}

// delaySeconds converts a host's crawl delay in deciseconds to a whole-second
// politeness interval, never less than one second so a zero-configured host
// still spaces its fetches.
func delaySeconds(deciseconds uint16) uint32 {
	s := uint32(deciseconds) / 10
	if s == 0 {
		return 1
	}
	return s
}
