package freshness

import (
	"math"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// ObserveChange decides whether one crawl saw a meaningful change, applying the
// trust order of section 7 and the longevity gate of section 5. It returns true
// only for a real, content-level change, so the change-rate estimate tracks
// meaningful change rather than cosmetic byte churn.
//
//   - A 304 (NotModified) is a definitive no-change, the cheapest signal.
//   - An identical content fingerprint is a no-change.
//   - A differing fingerprint within the simhash near-dup threshold is cosmetic
//     churn (a rotating ad, a ticking timestamp) and does not count.
//   - A differing fingerprint past the threshold is a meaningful change.
//
// A first fetch (no stored fingerprint) is not a change: there is nothing to
// compare against, so it only seeds the signal.
func ObserveChange(rec *meguri.URLRecord, o meguri.Outcome) bool {
	if o.NotModified {
		return false
	}
	if o.ContentFP == 0 || rec.ContentFP == 0 {
		return false
	}
	return dedup.ClassifyChange(rec.ContentFP, o.ContentFP, rec.Simhash, o.Simhash) == dedup.RealChange
}

// Estimate recomputes a URL's Poisson change rate lambda from its sufficient
// statistics (section 3): the no-change ratio over the crawl history, inverted
// through the Poisson no-change probability, smoothed against the small-sample
// degeneracy, and pulled toward the recent stability a no-change streak implies.
// It is a pure function of the record's counters and timestamps, the whole point
// of choosing an estimator with a tiny sufficient statistic.
//
// The naive count X/(nT) is biased low because a visit cannot distinguish one
// change from several between crawls; this estimator works off the no-change
// fraction, which is observed exactly, so it is consistent and does not saturate
// on fast pages (section 3, "Why the naive estimator is wrong").
func Estimate(rec *meguri.URLRecord, p Params) float64 {
	intervals := float64(rec.CrawlCount) - 1
	if intervals < 1 {
		// One crawl is zero intervals: nothing observed yet, so hold the page at
		// the floor (a slow re-probe) until a second crawl gives the first ratio.
		return p.MinRate
	}
	tavg := avgInterval(rec, intervals)
	if tavg <= 0 {
		return p.MinRate
	}

	// Smoothed no-change ratio: a Laplace pseudo-count keeps r strictly inside the
	// open interval so the log never sees zero or one (section 3, the honesty flag
	// on the bias-correction constant). Alpha is a tuned parameter, not a borrowed
	// exact paper constant.
	nochange := intervals - float64(rec.ChangeCount)
	if nochange < 0 {
		nochange = 0
	}
	r := (nochange + p.Alpha) / (intervals + 2*p.Alpha)
	lambdaLong := -(1.0 / tavg) * math.Log(r)

	lambdaRecent := recencyAdjust(lambdaLong, rec.NoChangeStreak, tavg, p)
	return clampRate(lambdaRecent, p.MinRate, p.MaxRate)
}

// avgInterval is the running average inter-crawl interval in hours, derived from
// the crawl-history span over the number of intervals. A single fixed lambda
// cannot track a non-stationary page, but the span-over-count average plus the
// recency adjustment lets the estimate drift as the page's behavior drifts
// without carrying a separate EWMA field per URL.
func avgInterval(rec *meguri.URLRecord, intervals float64) float64 {
	if rec.LastCrawled <= rec.FirstSeen {
		return 0
	}
	return float64(rec.LastCrawled-rec.FirstSeen) / intervals
}

// recencyAdjust pulls the long-run estimate toward the rate a recent no-change
// streak implies, so a page that was busy early but has gone quiet is scheduled
// as quiet (section 3, the recency weighting nochange_streak provides). A streak
// of s no-change intervals is itself a tiny no-change ratio, smoothed the same
// way, and a half-life weight blends the two: a short streak barely moves the
// estimate, a long one all but replaces it with the streak-implied rate.
func recencyAdjust(lambdaLong float64, streak uint16, tavg float64, p Params) float64 {
	if streak == 0 || tavg <= 0 {
		return lambdaLong
	}
	s := float64(streak)
	// The streak-implied rate: s consecutive no-change intervals, smoothed so the
	// log stays finite, give a rate that falls as the streak lengthens.
	rStreak := (s + p.Alpha) / (s + 2*p.Alpha)
	lambdaStreak := -(1.0 / tavg) * math.Log(rStreak)
	// Half-life weighting: weight on the long-run estimate decays with the streak,
	// so a streak of one half-life splits the difference.
	w := math.Exp(-s / p.RecencyHalfLife)
	blended := w*lambdaLong + (1-w)*lambdaStreak
	// Recency only ever pulls the rate down toward stability, never up.
	if blended > lambdaLong {
		return lambdaLong
	}
	return blended
}
