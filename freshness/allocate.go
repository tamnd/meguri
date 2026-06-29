package freshness

import (
	"math"

	"github.com/tamnd/meguri"
)

// marginalFreshnessGain is the marginal time-averaged freshness a page of change
// rate lambda earns from one more unit of crawl rate, evaluated at crawl rate f
// (section 4). It is the derivative of the Poisson freshness integral
//
//	F-bar(I) = (1 - e^{-lambda*I}) / (lambda*I)
//
// with respect to the crawl rate f = 1/I, and it is the analytic source of the
// hump: it falls to zero for near-static pages (lambda small, almost nothing to
// gain) and for hyper-volatile pages (lambda large, hopeless to keep fresh), and
// peaks in the medium band where a feasible crawl rate sits near the page's own
// change period. Writing x = lambda/f,
//
//	g(x)        = (1 - e^{-x} - x*e^{-x}) / x^2      (the positive -dF-bar/dx)
//	dF-bar/df   = g(x) * lambda / f^2
//
// which is positive, decreasing in f (diminishing returns from crawling more),
// and humped in lambda at fixed f. Measuring it at the partition's feasible
// reference rate is what makes the value density non-monotone across pages, the
// shape the water-filling rule funds (doc 06, "the hump is in the
// freshness-per-crawl-budget across pages").
func marginalFreshnessGain(lambda, f float64) float64 {
	if lambda <= 0 || f <= 0 {
		return 0
	}
	x := lambda / f
	return gx(x) * lambda / (f * f)
}

// gx is (1 - e^{-x} - x*e^{-x}) / x^2, the positive negative-derivative of the
// freshness integral, continuous at zero where it tends to 1/2.
func gx(x float64) float64 {
	if x < 1e-9 {
		return 0.5 // limit as x -> 0
	}
	e := math.Exp(-x)
	return (1 - e - x*e) / (x * x)
}

// ValueDensity is the per-URL quantity the water-filling rule thresholds on: the
// importance-weighted marginal freshness gain a crawl buys, measured at the
// partition reference rate (section 4). It is high for medium-rate important
// pages and low for near-static and hyper-volatile pages alike, the hump. The
// importance weight is the embarrassment proxy (section 1), the page's Priority.
func ValueDensity(rec *meguri.URLRecord, p Params) float64 {
	w := float64(rec.Priority)
	if w <= 0 {
		w = 1 // an unweighted page still competes on its change behavior alone
	}
	lambda := clampRate(float64(rec.Lambda), p.MinRate, p.MaxRate)
	return w * marginalFreshnessGain(lambda, p.RefRate)
}

// fundedRate solves the water-filling equation for one funded URL: the crawl rate
// at which its importance-weighted marginal value falls to the global water level
// tau (section 4, 8). The marginal value decreases monotonically in the rate, so
// a bisection over the rate band converges quickly. A higher tau (a precious
// budget) yields a lower rate and a longer interval; a lower tau funds the page
// to a higher rate. This is the per-URL form of the Azar et al threshold rule,
// not the wrong square-root rule the research notes reject.
func fundedRate(rec *meguri.URLRecord, tau float64, p Params) float64 {
	w := float64(rec.Priority)
	if w <= 0 {
		w = 1
	}
	lambda := clampRate(float64(rec.Lambda), p.MinRate, p.MaxRate)

	lo, hi := p.MinRate, p.MaxRate
	// If even the ceiling rate is worth more than the water level, fund at the cap.
	if w*marginalFreshnessGain(lambda, hi) >= tau {
		return hi
	}
	// If even the floor rate is not worth the water level, the caller's threshold
	// test was borderline; fall back to the floor.
	if w*marginalFreshnessGain(lambda, lo) <= tau {
		return lo
	}
	for range 40 {
		mid := 0.5 * (lo + hi)
		if w*marginalFreshnessGain(lambda, mid) > tau {
			lo = mid // worth more than the water level: can afford to crawl faster
		} else {
			hi = mid
		}
	}
	return 0.5 * (lo + hi)
}

// hostRateShare is the per-host politeness cap C_h split across the host's URLs
// (the Kolobov et al constraint, section 4). A host with a crawl delay can be
// fetched at most once per delay, so its whole budget is 1/delay crawls per hour,
// and each of its URLs may claim a share of that. The share bounds the optimizer
// so the sum of a host's funded rates never exceeds what politeness allows, no
// matter how much global budget the water level would otherwise pour in.
func hostRateShare(h *meguri.HostRecord) float64 {
	delayHours := float64(h.CrawlDelay) / 10.0 / 3600.0 // deciseconds -> hours
	if delayHours <= 0 {
		delayHours = 1.0 / 3600.0 // a zero-configured host still spaces by one second
	}
	hostMax := 1.0 / delayHours // crawls/hour the host permits in total
	urls := max(float64(h.URLCount), 1)
	return hostMax / urls
}

// TargetInterval turns the allocation into a concrete recrawl interval in hours
// for one URL under the current water level tau and its host's politeness cap
// (section 4). A URL whose value density is at or below tau is starved to the
// slow re-probe rate; a URL above tau is funded to the rate where its marginal
// value meets tau. Either way the rate is clamped to the host's share of its
// politeness cap and to the global rate band, and the interval is the inverse.
func TargetInterval(rec *meguri.URLRecord, h *meguri.HostRecord, tau float64, p Params) float64 {
	var rate float64
	if ValueDensity(rec, p) <= tau {
		rate = p.ReprobeRate
	} else {
		rate = fundedRate(rec, tau, p)
	}
	if h != nil {
		rate = min(rate, hostRateShare(h))
	}
	rate = clampRate(rate, p.MinRate, p.MaxRate)
	return 1.0 / rate
}

// SetNextDue turns the optimizer's target interval into a concrete schedule time,
// deterministically (Coffman, Liu, Weber even spacing, not Poisson; section 6).
// It adds a small deterministic per-URL spread so a host's URLs do not all come
// due in the same hour and slam its politeness cap, which is the even-spacing
// principle applied across a host's URLs rather than across one URL's crawls. The
// monotonic discipline holds: next_due never lands in the past. now is in
// epoch-hours. It moves the URL to DueRecrawl, the Crawled -> DueRecrawl
// transition of the state machine (doc 03).
func SetNextDue(rec *meguri.URLRecord, intervalHours float64, now uint32, p Params) {
	interval := max(uint32(math.Round(intervalHours)), 1)
	due := rec.LastCrawled + interval + hostSpread(rec, interval, p)
	if due <= now {
		due = now + p.MinReprobeGap
	}
	rec.NextDue = due
	rec.Status = meguri.StatusDueRecrawl
}

// hostSpread is the deterministic jitter that staggers a host's due times. It is
// a fraction of the interval keyed on the URL's path, so two URLs of one host
// rarely come due in the same hour, but it is bounded below the interval so it
// never reshapes the schedule, and it is deterministic so a recovered checkpoint
// reproduces the same due times.
func hostSpread(rec *meguri.URLRecord, interval uint32, p Params) uint32 {
	window := min(p.HostSpreadWindow, interval) // never spread by more than the interval itself
	if window <= 1 {
		return 0
	}
	return uint32(rec.URLKey.PathKey % uint64(window))
}
