// Package freshness is the rescheduler, the heart of meguri (doc 06). It decides
// when each URL comes around again and why. It estimates a per-URL change rate
// from the crawl history with the Cho and Garcia-Molina no-change-ratio
// estimator, allocates the refresh budget by the non-monotone water-filling rule
// of Azar et al (refined by the Kolobov per-host politeness cap), spaces the
// crawls deterministically per Coffman, Liu, and Weber, and writes next_due back
// for the schedule.
//
// The whole model runs off a tiny sufficient statistic already on the record:
// CrawlCount, ChangeCount, NoChangeStreak, FirstSeen, LastCrawled, plus Lambda
// and NextDue it maintains. It reads the change signal the frontier classifies
// from each Outcome (304, content fingerprint, simhash longevity gate) and the
// importance weight from Priority, and produces a NextDue time and a refresh
// shadow price tau.
//
// This is the M4 milestone. The estimator and allocator are pure functions over
// meguri.URLRecord and meguri.HostRecord; the frontier wires them in opt-in
// through WithFreshness so the earlier milestones' dispatch sequences are
// unchanged when freshness is off.
package freshness
