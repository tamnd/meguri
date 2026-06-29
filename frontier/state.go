package frontier

import (
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// maxRetries is the retry-then-tombstone limit (doc 13, the state update; doc 03
// retry_count). A URL that fails transiently more than this many times in a row
// stops being re-queued and moves to Gone, so a permanently broken URL cannot
// occupy the frontier forever. RetryCount is a uint8, so this stays well clear of
// the overflow.
const maxRetries uint8 = 5

// markCrawled records a successful fetch (a 200, a 304, or a resolved redirect):
// the URL moves to Crawled, its history advances, its body signals fold into the
// change-rate counters, and its next_due comes from the rescheduler when one is
// on. This is the only path that increments crawl_count and spreads OPIC cash, so
// a failure never looks like a crawl to the freshness or importance models. It is
// also the whole of Report when the state machine is off, which keeps the earlier
// milestones' behavior exact.
func (f *Frontier) markCrawled(rec *meguri.URLRecord, h *hostEntry, o meguri.Outcome, now uint32) {
	rec.Status = meguri.StatusCrawled
	rec.RetryCount = 0 // a success clears the transient-failure streak
	if rec.FirstSeen == 0 {
		rec.FirstSeen = o.FetchedAt // anchor the history clock on the first crawl
	}
	rec.LastCrawled = o.FetchedAt
	rec.CrawlCount++
	rec.NextDue = o.FetchedAt + recrawlGapHours

	// Classify the change against the stored signals (doc 08, section 7.4). A
	// soft-404 template is recorded as Gone, restoring the stop signal the 200
	// tried to defeat (doc 08, section 8.6). A meaningful change increments the
	// change count and stamps last-changed, the signal doc 06's rate estimator
	// reads; a cosmetic change (simhash within Hamming 3) does not, so a rotating
	// ad never poisons lambda.
	if !o.NotModified && o.ContentFP != 0 {
		if f.soft.Observe(rec.HostKey, o.ContentFP, o.URLKey) {
			rec.Status = meguri.StatusGone
		}
		if rec.ContentFP != 0 { // a prior fetch left a signal to compare against
			switch dedup.ClassifyChange(rec.ContentFP, o.ContentFP, rec.Simhash, o.Simhash) {
			case dedup.NoChange, dedup.CosmeticChange:
				rec.NoChangeStreak++
			case dedup.RealChange:
				rec.ChangeCount++
				rec.NoChangeStreak = 0
				rec.LastChanged = o.FetchedAt
			}
		}
		rec.ContentFP = o.ContentFP
		rec.Simhash = o.Simhash
	}
	// A 304 is a no-change observation: the conditional GET saved the body and
	// the freshness model reads the streak (doc 06).
	if o.NotModified {
		rec.NoChangeStreak++
	}
	// Store fresh validators so the next fetch can go conditional.
	if o.ETag != "" {
		rec.ETagRef = f.arena.intern(o.ETag)
	}
	if o.LastModified != 0 {
		rec.LastModified = o.LastModified
	}
	// Freshness rescheduling (doc 06): with the rescheduler on, the change counters
	// updated just above feed the Poisson estimate, and the URL's next_due and
	// status come from the allocation rather than the flat M1 placeholder.
	if f.fresh != nil {
		f.reschedule(rec, h, now)
	}
	// Prioritization (doc 09): a crawled page distributes its accumulated OPIC cash
	// across its extracted out-links and ingests them, so importance flows to what
	// it links to and the front bank reorders as the crawl learns the graph. Off by
	// default, so the earlier milestones never run it.
	if f.prio != nil && len(o.Links) > 0 {
		f.spreadCash(rec, h, o.Links, now)
	}
	// Schedule index (doc 06, M6): file the recrawl. A page that stayed Crawled
	// re-enters the schedule as DueRecrawl when its NextDue hour arrives; a soft-404
	// that just tombstoned to Gone is not refiled, so a dead URL leaves the wheel.
	// Off by default, so the earlier milestones leave a crawl terminal.
	if f.wheelOn && rec.Status == meguri.StatusCrawled {
		f.wheel.add(rec.URLKey, rec.NextDue)
	}
}

// failURL handles a failed fetch (doc 13, the state update). A 410 Gone, or a
// failure past the retry limit, tombstones the URL: it moves to Gone, stays in
// the seen-set so it still dedups rediscoveries, and is not re-queued, so it
// leaves the frontier. Any other failure (a transient 429/5xx or a Retryable
// transport error, or a 404 that has not yet hit the limit) backs off and
// re-queues: retry_count rises, next_due is pushed out by an exponential backoff,
// and the key goes to the back of its host queue so the rest of the host's work
// runs first and the retry rides the host's AIMD-widened window. The lifetime
// error_count rises on every failure.
func (f *Frontier) failURL(rec *meguri.URLRecord, h *hostEntry, o meguri.Outcome) {
	if rec.ErrorCount < ^uint16(0) {
		rec.ErrorCount++
	}
	if rec.RetryCount < maxRetries {
		rec.RetryCount++
	}
	// 410 is a definitive tombstone; so is exhausting the retry budget on a
	// persistent failure (a 404 that never recovers, a server that stays down).
	if o.HTTPStatus == 410 || rec.RetryCount >= maxRetries {
		rec.Status = meguri.StatusGone
		return
	}
	// Transient or re-probable: schedule another attempt behind a growing backoff
	// and re-queue the key so the host fetches it again.
	rec.Status = meguri.StatusScheduled
	rec.NextDue = o.FetchedAt + retryBackoffHours(rec.RetryCount)
	if h != nil {
		h.back = append(h.back, rec.URLKey)
	}
}

// retryBackoffHours is the exponential backoff between retries, in epoch-hours:
// 1, 2, 4, 8 hours for the first attempts, capped at the flat recrawl gap so a
// retry never schedules further out than a normal recrawl. The host's AIMD
// interval handles the short-term spacing inside a run; this is the durable
// next_due a recovery or a later campaign reads.
func retryBackoffHours(retry uint8) uint32 {
	if retry == 0 {
		retry = 1
	}
	return min(uint32(1)<<(retry-1), recrawlGapHours)
}

// recordRedirect stores a redirect (doc 13, the state update; doc 03 redirect
// handling). The resolved target is canonicalized and keyed exactly as a crawled
// out-link is, looked up or created as its own URLRecord through the idempotent
// discovery path (so a redirect to a known URL just dedups), and the source's
// redirect_ref is pointed at the target's canonical URL in the string arena so
// the link survives a checkpoint. The target inherits the source's depth (a
// redirect is the same resource moved, not a step deeper) and carries the
// source's priority as its discovery weight. A target whose scheme or shape is
// not crawlable is dropped, leaving only http_status on the source.
func (f *Frontier) recordRedirect(rec *meguri.URLRecord, o meguri.Outcome, now uint32) {
	if o.RedirectTarget == "" {
		return
	}
	base := f.arena.str(rec.URLRef)
	key, canon, _, ok := dedup.CanonicalKey(o.RedirectTarget, base, meguri.GroupRegistrableDomain, nil)
	if !ok {
		return
	}
	f.Discover(meguri.Discovery{
		URLKey:          key,
		CanonicalURL:    canon,
		Depth:           rec.Depth,
		DiscoverySource: meguri.SourceRedirect,
		SrcHostKey:      rec.HostKey,
		LinkWeight:      rec.Priority,
		ObservedAt:      o.FetchedAt,
	}, now)
	rec.RedirectRef = f.arena.intern(canon)
}
