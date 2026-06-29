package frontier

import (
	"time"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/politeness"
	"github.com/tamnd/meguri/robots"
)

// robotsTTLHours is how long parsed robots rules are trusted before a re-fetch.
// RFC 9309 suggests a 24h cache; that is the default crawl-to-crawl horizon.
const robotsTTLHours = 24

// ipTTLHours is the fallback DNS cache lifetime stamped on a host record when the
// resolver lands an address. The resolver clamps the real record TTL internally;
// this is only the durable hint carried in the .meguri file.
const ipTTLHours = 1

// newHost builds a host entry with its politeness state seeded: the adaptive
// interval starts at the policy baseline and the floor is the configured crawl
// delay. When DNS is on, the host name is queued for resolution off the dispatch
// path so its address is ready by the time it dispatches.
func (f *Frontier) newHost(hk, ref uint64, hostName string, crawlDelay uint16) *hostEntry {
	h := &hostEntry{
		rec: meguri.HostRecord{
			HostKey:        hk,
			HostRef:        ref,
			Grouping:       meguri.GroupRegistrableDomain,
			RegistrableRef: ref,
			CrawlDelay:     crawlDelay,
		},
		effective:  f.pol.Default,
		crawlFloor: deciToDur(crawlDelay),
	}
	if f.resolver != nil && hostName != "" {
		f.resolver.Prefetch(hostName)
	}
	return h
}

// resolveHost fills in the host's cached IP from the resolver when DNS is on and
// the address is missing or stale. Resolution itself happened off the dispatch
// path in the prefetch pool; this only reads the cache, so a dispatch never
// blocks on DNS. A host with no address yet crawls host-only until one lands.
func (f *Frontier) resolveHost(h *hostEntry, now uint32) {
	if f.resolver == nil {
		return
	}
	if h.rec.ResolvedIP != ([16]byte{}) && h.rec.IPExpiry > now/3600 {
		return
	}
	host := f.arena.str(h.rec.HostRef)
	if ip, ok := f.resolver.Lookup(host); ok {
		h.rec.ResolvedIP = ip
		h.rec.IPExpiry = now/3600 + ipTTLHours
	}
}

// spend advances both politeness buckets for a dispatch at now: the host bucket
// by the clamped adaptive interval, the shared per-IP bucket by the same step
// (never under its own floor). The host's next-eligible instant is the later of
// the two, so a host on a busy IP waits for whichever throttle is tighter.
func (f *Frontier) spend(h *hostEntry, now uint32) {
	interval := f.pol.HostInterval(h.effective, h.crawlFloor)
	hostNext := now + durSecs(interval)
	f.ips.Spend(h.rec.ResolvedIP, int64(now), interval)
	ipNext := uint32(f.ips.EligibleAt(h.rec.ResolvedIP))
	h.rec.HostNextEligible = hostNext
	h.rec.IPNextEligible = ipNext
}

// eligibleNow is the live next-eligible instant of a host, the later of its own
// politeness window and the current per-IP window. It is recomputed on every
// placement because another host sharing the IP may have advanced that bucket
// since this host was last filed.
func (f *Frontier) eligibleNow(h *hostEntry) uint32 {
	e := h.rec.HostNextEligible
	if ip := uint32(f.ips.EligibleAt(h.rec.ResolvedIP)); ip > e {
		e = ip
	}
	return e
}

// adapt folds one fetch outcome into the host's adaptive interval (AIMD) and its
// smoothed latency. A host pinned at the ceiling across repeated errors is
// flagged dead. The configured floor in CrawlDelay is left untouched: the
// adaptive interval floats above it and resets on recovery.
func (f *Frontier) adapt(h *hostEntry, o meguri.Outcome) {
	sig := politeness.Signal{
		Status:      int(o.HTTPStatus),
		RetryAfter:  deciToDur(o.RetryAfter),
		Latency:     time.Duration(o.LatencyMS) * time.Millisecond,
		PrevLatency: time.Duration(h.rec.AvgLatency) * time.Millisecond,
	}
	h.effective = f.pol.HostInterval(f.pol.Adapt(h.effective, sig), h.crawlFloor)
	h.rec.AvgLatency = smoothLatency(h.rec.AvgLatency, o.LatencyMS)
	if o.HTTPStatus > 0 {
		h.rec.CrawlTotal++
	}
	if isErr(o.HTTPStatus) {
		h.rec.ErrorTotal++
	}

	if f.pol.AtCeiling(h.effective) && isErr(o.HTTPStatus) {
		if h.ceilStreak < 255 {
			h.ceilStreak++
		}
		if h.ceilStreak >= 3 {
			h.rec.Flags |= meguri.HostFlagDeadHost
		}
	} else {
		h.ceilStreak = 0
	}
}

// needsRobots reports whether a host must fetch robots.txt before its next
// content URL: robots is on, the host has content work, and its rules are either
// never fetched or past their cache expiry. A fetch already in flight is not
// re-requested.
func (f *Frontier) needsRobots(h *hostEntry, now uint32) bool {
	if h.robotsState == robotsPending || len(h.back) == 0 {
		return false
	}
	if h.robotsState == robotsReady && h.rec.RobotsExpiry > now/3600 {
		return false
	}
	return true
}

// robotsRequest builds the robots.txt fetch for a host. The scheme is https by
// default; ami follows a redirect to http if the host only serves there.
func (f *Frontier) robotsRequest(h *hostEntry) fetch.Request {
	host := f.arena.str(h.rec.HostRef)
	return fetch.Request{
		URLKey:       robotsKey(h.rec.HostKey),
		HostKey:      h.rec.HostKey,
		CanonicalURL: "https://" + host + "/robots.txt",
		ResolvedIP:   h.rec.ResolvedIP,
		Robots:       true,
	}
}

// applyRobots turns a robots.txt outcome into cached rules and lets the host
// proceed to its content URLs (doc 07, the error handling table):
//   - 2xx parses and caches the rules, and a robots Crawl-delay raises the host
//     floor durably.
//   - 4xx means no robots.txt, so the host is allow-all and flagged missing.
//   - 5xx or a transport error allows the host with a short retry window rather
//     than blocking it forever; the next dispatch re-fetches robots.
//
// Already-queued URLs that the new rules disallow are excluded, then the host is
// re-placed behind its politeness window.
func (f *Frontier) applyRobots(h *hostEntry, o meguri.Outcome, now uint32) {
	switch {
	case o.HTTPStatus >= 200 && o.HTTPStatus < 300:
		h.robots = robots.Parse(o.RobotsBody, f.agent)
		h.rec.Flags &^= meguri.HostFlagRobotsMissing
		if cd := h.robots.CrawlDelay(); cd > 0 {
			if d := durToDeci(cd); d > h.rec.CrawlDelay {
				h.rec.CrawlDelay = d
				h.crawlFloor = deciToDur(d)
			}
		}
		h.rec.RobotsExpiry = now/3600 + robotsTTLHours
	case o.HTTPStatus >= 400 && o.HTTPStatus < 500:
		h.robots = nil
		h.rec.Flags |= meguri.HostFlagRobotsMissing
		h.rec.RobotsExpiry = now/3600 + robotsTTLHours
	default:
		h.robots = nil
		h.rec.RobotsExpiry = now/3600 + 1 // short retry window on a 5xx or error
	}
	h.robotsState = robotsReady
	h.rec.RobotsFetched = now / 3600
	h.inFlight = false
	f.filterRobots(h)
	f.place(h, now)
}

// filterRobots drops the host's queued URLs that the freshly parsed rules
// disallow, marking each excluded so the row stays for dedup but never
// dispatches.
func (f *Frontier) filterRobots(h *hostEntry) {
	if h.robots == nil {
		return
	}
	kept := h.back[:0]
	for _, k := range h.back {
		if f.allowed(h, k) {
			kept = append(kept, k)
			continue
		}
		f.records[k].Status = meguri.StatusExcludedRobots
	}
	h.back = kept
}

// allowed reports whether a host's robots rules permit a URL. A host with no
// rules (allow-all, or robots off) permits everything.
func (f *Frontier) allowed(h *hostEntry, key meguri.URLKey) bool {
	if h.robots == nil {
		return true
	}
	rec := f.records[key]
	if rec == nil {
		return true
	}
	return h.robots.Allowed(PathOf(f.arena.str(rec.URLRef)))
}

// robotsKey is the synthetic URLKey of a host's robots.txt, the key its robots
// outcome echoes back so Report can tell a robots fetch from a content fetch.
func robotsKey(hk uint64) meguri.URLKey {
	return meguri.URLKey{HostKey: hk, PathKey: meguri.PathKeyOf("/robots.txt")}
}

// deciToDur converts a crawl delay in deciseconds to a Duration.
func deciToDur(deci uint16) time.Duration {
	return time.Duration(deci) * 100 * time.Millisecond
}

// durToDeci converts a Duration to whole deciseconds, clamped to the uint16 the
// record column holds.
func durToDeci(d time.Duration) uint16 {
	v := d / (100 * time.Millisecond)
	if v < 0 {
		return 0
	}
	if v > 65535 {
		return 65535
	}
	return uint16(v)
}

// durSecs rounds an interval down to whole seconds, the granularity of the
// epoch-second eligibility clock, never below one so a host always spaces its
// fetches. It mirrors delaySeconds so the wired interval matches the M1 floor.
func durSecs(d time.Duration) uint32 {
	s := uint32(d / time.Second)
	if s == 0 {
		return 1
	}
	return s
}

// smoothLatency is an exponential moving average (alpha 1/4) of fetch latency in
// milliseconds, the signal AIMD reads to widen before a slow host starts
// erroring. The first sample seeds the average.
func smoothLatency(prev, sample uint16) uint16 {
	if prev == 0 {
		return sample
	}
	return uint16((uint32(prev)*3 + uint32(sample)) / 4)
}

// isErr reports whether a status is a backoff trigger: a 429 rate limit or a 5xx
// server error.
func isErr(status uint16) bool {
	return status == 429 || (status >= 500 && status <= 599)
}
