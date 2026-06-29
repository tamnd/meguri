package frontier

import (
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/prioritize"
)

// This file is the M5 prioritization wiring (doc 09). The prioritize package owns
// the policy: the online OPIC cash flow, the import blend, the STAR cross-host
// budget, and the spam penalties. The frontier owns the seams: a seed endows its
// cash, a discovery credits cash and reputation and admits under the budget, and
// a crawl distributes the source's cash across its out-links and ingests them.
// Everything here is reached only when WithPrioritizer set f.prio, so the earlier
// milestones' dispatch order is unchanged when prioritization is off.

// creditDiscovery folds a rediscovered out-link's importance into a resident URL
// (doc 09): it credits the OPIC cash the link carries and counts its cross-host
// in-degree, refreshes the target host's STAR budget from the new reputation, and
// reprices the URL, re-bucketing it in the front bank if its priority crossed a
// level. This is the path the seen-set sends a duplicate down: the row already
// exists, only the signals move.
func (f *Frontier) creditDiscovery(d meguri.Discovery, rec *meguri.URLRecord) {
	indeg := f.prio.Credit(d)
	h := f.hosts[d.URLKey.HostKey]
	if h != nil {
		prioritize.UpdateHostBudget(&h.rec, indeg, f.prio.Params())
	}
	f.reprice(rec, h)
}

// reprice recomputes a URL's blended, penalized priority and, when the URL is
// still waiting in the front bank, re-buckets it to match (doc 09's rate-limited
// re-bucketing: rebucket is a no-op unless the priority crossed a level). A URL
// already bound to a host back queue or crawled is not in the front bank, so its
// stored priority is simply refreshed for the next time it enters.
func (f *Frontier) reprice(rec *meguri.URLRecord, h *hostEntry) {
	var hr *meguri.HostRecord
	if h != nil {
		hr = &h.rec
	}
	old := rec.Priority
	rec.Priority = f.prio.Priority(rec, hr)
	switch rec.Status {
	case meguri.StatusScheduled, meguri.StatusReady, meguri.StatusDueRecrawl:
		f.urlFront.rebucket(rec.URLKey, old, rec.Priority)
	}
}

// spreadCash runs one OPIC visit for a just-crawled page (doc 09): Distribute
// folds the source's held cash into its discounted history and splits the cash
// across its extracted out-links, writing each link's LinkWeight, then every link
// is routed through the idempotent discovery intake, which credits the cash to
// its target. In a single partition every link is local; the doc 12 router sends
// a cross-partition link to its owner, where the same credit runs. The source's
// own priority is refreshed last, its history having grown.
func (f *Frontier) spreadCash(rec *meguri.URLRecord, h *hostEntry, links []meguri.Discovery, now uint32) {
	f.prio.Distribute(rec.URLKey, links)
	for i := range links {
		f.Discover(links[i], now)
	}
	f.reprice(rec, h)
}
