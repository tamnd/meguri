package frontier

import (
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/freshness"
)

// reschedule runs the freshness loop for one crawled URL (doc 06, section 9). It
// recomputes the URL's Poisson change rate from the sufficient statistics the
// change classification just updated, turns the rate into an optimal recrawl
// interval under the current water level tau and the host's politeness cap, and
// spaces the next_due deterministically. The water level is re-tuned in the
// background on a slow cadence so the per-crawl path stays O(1) on the estimate
// and the allocation, never a stop-the-world solve. now is epoch-seconds; the
// schedule works in epoch-hours.
func (f *Frontier) reschedule(rec *meguri.URLRecord, h *hostEntry, now uint32) {
	rec.Lambda = float32(freshness.Estimate(rec, *f.fresh))

	var hr *meguri.HostRecord
	if h != nil {
		hr = &h.rec
	}
	tau := 0.0
	if f.tau != nil {
		tau = f.tau.Tau()
	}
	interval := freshness.TargetInterval(rec, hr, tau, *f.fresh)
	freshness.SetNextDue(rec, interval, now/3600, *f.fresh)

	// Re-tune the water level on a slow cadence. The tick samples the partition's
	// total scheduled refresh rate at the current tau and nudges tau toward the
	// budget, the incremental stand-in for a global re-solve (section 8).
	if f.tau != nil {
		f.reschedN++
		if f.reschedN >= tauTickEvery {
			f.tau.Tick(f.scheduledRate())
			f.reschedN = 0
		}
	}
}

// scheduledRate is the partition's total refresh crawl rate at the current water
// level, the aggregate the tau controller drives toward the budget (doc 06,
// section 8). It sums the optimizer's per-URL rate over the URLs that carry a
// freshness schedule, the crawled and due-for-recrawl rows. It is an O(N) sweep
// run only on the slow tau cadence, not on the dispatch path.
func (f *Frontier) scheduledRate() float64 {
	tau := f.tau.Tau()
	var sum float64
	for _, rec := range f.records {
		if rec.Status != meguri.StatusCrawled && rec.Status != meguri.StatusDueRecrawl {
			continue
		}
		var hr *meguri.HostRecord
		if h := f.hosts[rec.HostKey]; h != nil {
			hr = &h.rec
		}
		sum += 1.0 / freshness.TargetInterval(rec, hr, tau, *f.fresh)
	}
	return sum
}
