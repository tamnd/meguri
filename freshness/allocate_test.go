package freshness

import (
	"testing"

	"github.com/tamnd/meguri"
)

// urlAt builds a record with a given change rate and importance weight, the two
// inputs the allocation reads.
func urlAt(lambda, weight float64) *meguri.URLRecord {
	return &meguri.URLRecord{Lambda: float32(lambda), Priority: float32(weight)}
}

// TestMarginalGainIsHumped is the central non-monotonicity (doc 06, section 4):
// the marginal freshness gain at a fixed reference rate is near zero for a
// near-static page and a hyper-volatile page alike and peaks in the medium band.
func TestMarginalGainIsHumped(t *testing.T) {
	f := DefaultParams().RefRate
	slow := marginalFreshnessGain(1.0/(40*24), f) // once / 40 days
	medium := marginalFreshnessGain(1.0/24, f)    // once / day, near the reference
	hyper := marginalFreshnessGain(12, f)         // every 5 minutes

	if !(medium > slow && medium > hyper) {
		t.Fatalf("marginal gain not humped: slow=%.6g medium=%.6g hyper=%.6g", slow, medium, hyper)
	}
	// Both tails are genuinely small relative to the peak, not just lower.
	if slow > medium/2 || hyper > medium/2 {
		t.Errorf("tails not suppressed: slow=%.6g hyper=%.6g peak=%.6g", slow, hyper, medium)
	}
}

// TestValueDensityStarvesBothExtremes checks the doc 06 section-10 verdicts at
// equal weight: the medium product page outranks both the near-static docs page
// and the hyper-volatile scoreboard.
func TestValueDensityStarvesBothExtremes(t *testing.T) {
	p := DefaultParams()
	docs := ValueDensity(urlAt(1.0/(30*24), 1), p) // ~once / 30 days
	product := ValueDensity(urlAt(1.0/48, 1), p)   // once / 2 days
	scoreboard := ValueDensity(urlAt(12, 1), p)    // every 5 minutes

	if !(product > docs && product > scoreboard) {
		t.Fatalf("medium page did not win: docs=%.6g product=%.6g scoreboard=%.6g", docs, product, scoreboard)
	}
}

// TestImportanceRescuesSlowNotHyper is the doc 06 section-10 importance table
// (lines 1084-1094): a tenfold weight scales both pages' value density tenfold,
// but the hump is not erased, so the slow page can be lifted across the water
// level into the funded band while the hyper-volatile page stays starved no
// matter the weight, because no feasible crawl rate makes it fresh.
func TestImportanceRescuesSlowNotHyper(t *testing.T) {
	p := DefaultParams()
	slowURL := func(w float64) *meguri.URLRecord { return urlAt(1.0/(30*24), w) }
	hyperURL := func(w float64) *meguri.URLRecord { return urlAt(12, w) }

	slowLight := ValueDensity(slowURL(1), p)
	slowHeavy := ValueDensity(slowURL(10), p)
	hyperLight := ValueDensity(hyperURL(1), p)
	hyperHeavy := ValueDensity(hyperURL(10), p)

	// Weight scales the value density linearly for both pages.
	if !almostEqual(slowHeavy, 10*slowLight, 10*slowLight*1e-6) {
		t.Errorf("weight did not scale the slow page linearly: %.6g vs 10*%.6g", slowHeavy, slowLight)
	}
	if !almostEqual(hyperHeavy, 10*hyperLight, 10*hyperLight*1e-6+1e-30) {
		t.Errorf("weight did not scale the hyper page linearly: %.6g vs 10*%.6g", hyperHeavy, hyperLight)
	}
	// The hump survives equal weight: at the same weight the hyper page sits below
	// the slow page, so no weight that funds the slow page also funds the hyper one
	// at that weight.
	if !(hyperHeavy < slowHeavy) {
		t.Fatalf("hump erased by weight: hyperHeavy=%.6g should stay below slowHeavy=%.6g", hyperHeavy, slowHeavy)
	}
	// Set the water level between the two weighted densities. The slow page is
	// rescued into funding (a short interval) while the hyper page, even at the
	// same weight, stays parked on the slow re-probe interval.
	tau := 0.5 * (hyperHeavy + slowHeavy)
	reprobe := 1.0 / p.ReprobeRate
	slowInterval := TargetInterval(slowURL(10), nil, tau, p)
	hyperInterval := TargetInterval(hyperURL(10), nil, tau, p)
	if !(slowInterval < reprobe) {
		t.Errorf("importance did not rescue the slow page: interval %.3fh is not under the re-probe %.3fh", slowInterval, reprobe)
	}
	if !almostEqual(hyperInterval, reprobe, 1e-6) {
		t.Errorf("weight wrongly rescued the hyper page: interval %.3fh, want the re-probe %.3fh", hyperInterval, reprobe)
	}
}

// TestFundedRateFallsWithTau checks the water-filling direction: a higher water
// level (a more precious budget) funds a page to a lower rate, a longer interval.
func TestFundedRateFallsWithTau(t *testing.T) {
	p := DefaultParams()
	u := urlAt(1.0/48, 1) // a medium page
	vd := ValueDensity(u, p)

	lowTau := fundedRate(u, vd*0.25, p)
	highTau := fundedRate(u, vd*0.9, p)
	if !(lowTau > highTau) {
		t.Fatalf("funded rate did not fall as tau rose: lowTau=%.6g highTau=%.6g", lowTau, highTau)
	}
}

// TestTargetIntervalStarvesBelowThreshold checks a URL whose value density is
// below the water level is parked on the slow re-probe interval, not funded.
func TestTargetIntervalStarvesBelowThreshold(t *testing.T) {
	p := DefaultParams()
	u := urlAt(12, 1) // hyper-volatile, low value density
	vd := ValueDensity(u, p)
	tau := vd * 2 // above the page's density: starved

	got := TargetInterval(u, nil, tau, p)
	want := 1.0 / p.ReprobeRate
	if !almostEqual(got, want, 1e-6) {
		t.Fatalf("starved interval = %.3fh, want the re-probe interval %.3fh", got, want)
	}
}

// TestHostCapClampsRate checks the Kolobov per-host constraint: a host with a
// one-hour crawl delay holding a single URL caps that URL's rate at one per hour,
// so even a richly funded URL cannot be scheduled faster than politeness allows.
func TestHostCapClampsRate(t *testing.T) {
	p := DefaultParams()
	u := urlAt(1.0/24, 100)                                 // high weight, would fund fast
	h := &meguri.HostRecord{CrawlDelay: 36000, URLCount: 1} // 3600s = 1h delay
	got := TargetInterval(u, h, 1e-12, p)                   // tau ~0: maximally funded
	if got < 1.0 {
		t.Fatalf("host cap not honored: interval %.3fh is under the 1h politeness floor", got)
	}
}

// TestSetNextDueEvenSpacing checks deterministic spacing: next_due is last_crawled
// plus the interval (plus a bounded host spread), never in the past, and the URL
// moves to DueRecrawl.
func TestSetNextDueEvenSpacing(t *testing.T) {
	p := DefaultParams()
	u := &meguri.URLRecord{
		URLKey:      meguri.URLKey{PathKey: 7},
		LastCrawled: 1000,
	}
	SetNextDue(u, 48, 1000, p)

	if u.Status != meguri.StatusDueRecrawl {
		t.Errorf("status = %v, want DueRecrawl", u.Status)
	}
	// 1000 + 48 + spread, spread in [0, min(window,48)).
	if u.NextDue < 1048 || u.NextDue >= 1048+p.HostSpreadWindow {
		t.Errorf("next_due = %d, want in [1048, %d)", u.NextDue, 1048+p.HostSpreadWindow)
	}
}

// TestSetNextDueNeverInPast checks the monotonic discipline: a tiny interval that
// would land at or before now is pushed to now plus the minimum gap.
func TestSetNextDueNeverInPast(t *testing.T) {
	p := DefaultParams()
	u := &meguri.URLRecord{LastCrawled: 10}
	SetNextDue(u, 1, 100, p) // due would be ~11, well before now=100
	if u.NextDue <= 100 {
		t.Fatalf("next_due = %d landed in the past (now=100)", u.NextDue)
	}
	if u.NextDue != 100+p.MinReprobeGap {
		t.Errorf("next_due = %d, want now+gap = %d", u.NextDue, 100+p.MinReprobeGap)
	}
}

// TestSetNextDueDeterministic checks two URLs of one host get different but stable
// due times, the even-spacing-across-a-host anti-herd spread.
func TestSetNextDueDeterministic(t *testing.T) {
	p := DefaultParams()
	a := &meguri.URLRecord{URLKey: meguri.URLKey{PathKey: 3}, LastCrawled: 0}
	b := &meguri.URLRecord{URLKey: meguri.URLKey{PathKey: 8}, LastCrawled: 0}
	SetNextDue(a, 48, 0, p)
	SetNextDue(b, 48, 0, p)
	if a.NextDue == b.NextDue {
		t.Errorf("two URLs of a host landed on the same due hour %d (no spread)", a.NextDue)
	}
	// Stable: recomputing gives the same answer.
	prev := a.NextDue
	a2 := &meguri.URLRecord{URLKey: meguri.URLKey{PathKey: 3}, LastCrawled: 0}
	SetNextDue(a2, 48, 0, p)
	if a2.NextDue != prev {
		t.Errorf("spread not deterministic: %d then %d", prev, a2.NextDue)
	}
}
