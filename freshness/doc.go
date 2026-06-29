// Package freshness is the rescheduler: it estimates each URL's change rate from
// its crawl history and decides when the URL is next due, so a page that changes
// hourly is revisited often and one that never changes drifts to the back. It
// maintains the Lambda, ChangeCount, NoChangeStreak, LastChanged, and NextDue
// fields of meguri.URLRecord using a Poisson change model updated from every
// meguri.Outcome, including the 304 not-modified observations conditional GET
// makes cheap.
//
// This is the M4 milestone. The package is a placeholder until then.
package freshness
