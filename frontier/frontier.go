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

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/format"
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

// hostEntry is the resident state of one host: its durable record, its back
// queue of URLs waiting to dispatch (FIFO, fed in priority order so the head is
// the host's best URL), and the live politeness and pool bookkeeping.
type hostEntry struct {
	rec      meguri.HostRecord
	back     []meguri.URLKey // FIFO; head is the next URL to dispatch for this host
	eligible uint32          // epoch-seconds, mirror of rec.HostNextEligible
	inFlight bool            // a URL of this host is dispatched, awaiting an outcome
	active   bool            // holds a back queue, counts against target
}

// Frontier is the single-partition resident scheduler.
type Frontier struct {
	id      uint32
	created uint32
	codec   uint8

	records map[meguri.URLKey]*meguri.URLRecord
	hosts   map[uint64]*hostEntry
	arena   arena

	urlFront   prioRing[meguri.URLKey] // URLs not yet bound to a host back queue
	readyHosts prioRing[uint64]        // hosts eligible now, keyed by best URL priority
	wait       waitHeap                // hosts parked until their politeness window opens

	target int // active-host cap (distributor)
	active int // hosts currently holding a back queue
}

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
		target:  defaultTarget,
	}
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
	if _, dup := f.records[key]; dup {
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
		ref := f.arena.intern(host)
		h = &hostEntry{rec: meguri.HostRecord{
			HostKey:        hk,
			HostRef:        ref,
			Grouping:       meguri.GroupRegistrableDomain,
			RegistrableRef: ref,
			CrawlDelay:     crawlDelay,
		}}
		f.hosts[hk] = h
	}
	h.rec.URLCount++
	f.urlFront.push(key, priority)
}

// Dispatch returns the next URL to fetch at clock time now (epoch-seconds), or
// ok=false when no host may be fetched at now. A false result does not mean the
// frontier is drained: call NextEligible to learn whether advancing the clock
// would open a host. The caller fetches the returned Request and feeds the
// outcome back through Report.
func (f *Frontier) Dispatch(now uint32) (fetch.Request, bool) {
	f.promote(now)
	f.distribute(now)
	hk, ok := f.readyHosts.pop()
	if !ok {
		return fetch.Request{}, false
	}
	h := f.hosts[hk]
	key := h.back[0]
	rec := f.records[key]
	rec.Status = meguri.StatusInFlight
	h.inFlight = true
	h.eligible = now + delaySeconds(h.rec.CrawlDelay)
	h.rec.HostNextEligible = h.eligible
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
	rec := f.records[o.URLKey]
	if rec == nil {
		return
	}
	h := f.hosts[rec.HostKey]
	if h != nil && len(h.back) > 0 && h.back[0] == o.URLKey {
		h.back = h.back[1:]
	}
	rec.Status = meguri.StatusCrawled
	rec.HTTPStatus = o.HTTPStatus
	rec.LastCrawled = o.FetchedAt
	rec.CrawlCount++
	rec.NextDue = o.FetchedAt + recrawlGapHours
	if o.ContentFP != 0 {
		rec.ContentFP = o.ContentFP
	}
	if o.Simhash != 0 {
		rec.Simhash = o.Simhash
	}
	if h == nil {
		return
	}
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
	if h.eligible <= now {
		f.readyHosts.push(h.rec.HostKey, f.records[h.back[0]].Priority)
		return
	}
	f.wait.push(h.rec.HostKey, h.eligible)
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
// its live politeness time folded back in, and the string arena. Recover rebuilds
// an identical scheduler from it.
func (f *Frontier) Checkpoint() *format.Partition {
	urls := make([]meguri.URLRecord, 0, len(f.records))
	for _, r := range f.records {
		urls = append(urls, *r)
	}
	sort.Slice(urls, func(i, j int) bool { return urls[i].URLKey.Less(urls[j].URLKey) })

	hosts := make([]meguri.HostRecord, 0, len(f.hosts))
	for _, h := range f.hosts {
		h.rec.HostNextEligible = h.eligible
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
		f.hosts[h.HostKey] = &hostEntry{rec: h, eligible: h.HostNextEligible}
	}
	for i := range p.URLs {
		rec := p.URLs[i]
		if rec.Status == meguri.StatusInFlight {
			rec.Status = meguri.StatusScheduled
		}
		r := rec
		f.records[r.URLKey] = &r
		switch r.Status {
		case meguri.StatusScheduled, meguri.StatusReady, meguri.StatusDueRecrawl:
			if f.hosts[r.HostKey] == nil {
				ref := f.arena.intern(HostOf(f.arena.str(r.URLRef)))
				f.hosts[r.HostKey] = &hostEntry{rec: meguri.HostRecord{
					HostKey: r.HostKey, HostRef: ref, RegistrableRef: ref, CrawlDelay: 10,
				}}
			}
			f.urlFront.push(r.URLKey, r.Priority)
		}
	}
	return f
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
