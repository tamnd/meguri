package freshness

import (
	"math"
	"testing"

	"github.com/tamnd/meguri"
)

// rec builds a URL record with a crawl history: n intervals over a span of
// n*Thours, of which changed saw a meaningful change, ending in a streak of
// streak no-change intervals.
func rec(intervals, changed, streak int, Thours uint32) *meguri.URLRecord {
	return &meguri.URLRecord{
		CrawlCount:     uint32(intervals + 1),
		ChangeCount:    uint32(changed),
		NoChangeStreak: uint16(streak),
		FirstSeen:      0,
		LastCrawled:    uint32(intervals) * Thours,
		Priority:       1,
	}
}

// TestEstimatorBeatsNaiveOnSlowPage walks the doc 06 section-3 slow example: 10
// intervals of 24 hours, 2 changes, 8 no-change. The naive count underestimates
// because it folds multi-change intervals into one; the no-change-ratio estimator
// corrects it upward.
func TestEstimatorBeatsNaiveOnSlowPage(t *testing.T) {
	p := DefaultParams()
	u := rec(10, 2, 0, 24)
	got := Estimate(u, p)

	naive := 2.0 / (10.0 * 24.0) // X / (n*T) = 0.00833
	if got <= naive {
		t.Fatalf("estimator %.6f did not exceed the biased naive %.6f", got, naive)
	}
	// The unsmoothed correct value is -(1/24)ln(0.8) = 0.00930; smoothing nudges it
	// a little higher. Stay in a band around it.
	if got < 0.0090 || got > 0.0130 {
		t.Errorf("slow-page lambda %.6f outside the expected band [0.0090, 0.0130]", got)
	}
}

// TestEstimatorDivergesOnFastPage is the doc 06 regime where the two estimators
// part ways: a change on 9 of 10 intervals. The naive count saturates near 1/T
// while the no-change-ratio estimator reads the heavy multi-change intervals and
// reports a rate several times higher.
func TestEstimatorDivergesOnFastPage(t *testing.T) {
	p := DefaultParams()
	u := rec(10, 9, 0, 24)
	got := Estimate(u, p)

	naive := 9.0 / (10.0 * 24.0) // 0.0375
	if got < 2*naive {
		t.Fatalf("fast-page lambda %.6f should be well above twice the naive %.6f", got, naive)
	}
}

// TestRecencyPullsLambdaDown checks a recent no-change streak lowers the working
// rate below the streak-free long-run estimate: a page that was busy early but
// has gone quiet is scheduled as quiet (doc 06, section 3 recency weighting).
func TestRecencyPullsLambdaDown(t *testing.T) {
	p := DefaultParams()
	noStreak := Estimate(rec(10, 2, 0, 24), p)
	streak := Estimate(rec(10, 2, 6, 24), p)
	if streak >= noStreak {
		t.Fatalf("a 6-interval no-change streak did not pull lambda down: %.6f >= %.6f", streak, noStreak)
	}
}

// TestNeverChangedGetsFloor checks a page seen to change zero times in a long
// history still gets the re-probe floor, not a rate of zero (which would schedule
// it never).
func TestNeverChangedGetsFloor(t *testing.T) {
	p := DefaultParams()
	got := Estimate(rec(20, 0, 20, 24), p)
	if got < p.MinRate {
		t.Fatalf("never-changed lambda %.8f fell below the floor %.8f", got, p.MinRate)
	}
	if got > 0.01 {
		t.Errorf("never-changed lambda %.8f is implausibly high for 20 quiet intervals", got)
	}
}

// TestFirstCrawlHoldsAtFloor checks a URL with a single crawl (no intervals yet)
// estimates the floor rate rather than dividing by zero.
func TestFirstCrawlHoldsAtFloor(t *testing.T) {
	p := DefaultParams()
	u := &meguri.URLRecord{CrawlCount: 1, FirstSeen: 0, LastCrawled: 0, Priority: 1}
	if got := Estimate(u, p); got != p.MinRate {
		t.Fatalf("single-crawl lambda = %.8f, want the floor %.8f", got, p.MinRate)
	}
}

// TestObserveChangeTrustOrder checks the section-7 trust order: a 304 and an
// identical fingerprint are no-change, a cosmetic byte change (simhash within the
// near-dup threshold) is no-change, a large simhash move is a real change, and a
// first fetch with nothing to compare against is not a change.
func TestObserveChangeTrustOrder(t *testing.T) {
	base := &meguri.URLRecord{ContentFP: 0x1111, Simhash: 0xFF00}

	if ObserveChange(base, meguri.Outcome{NotModified: true, ContentFP: 0x2222}) {
		t.Error("304 should be a definitive no-change")
	}
	if ObserveChange(base, meguri.Outcome{ContentFP: 0x1111, Simhash: 0xFF00}) {
		t.Error("identical fingerprint should be a no-change")
	}
	// One bit of simhash difference is within Hamming 3: cosmetic churn.
	if ObserveChange(base, meguri.Outcome{ContentFP: 0x2222, Simhash: 0xFF01}) {
		t.Error("a cosmetic byte change should not count")
	}
	// A far simhash is a real change.
	if !ObserveChange(base, meguri.Outcome{ContentFP: 0x2222, Simhash: 0x00FF}) {
		t.Error("a large simhash move should be a real change")
	}
	first := &meguri.URLRecord{} // nothing stored yet
	if ObserveChange(first, meguri.Outcome{ContentFP: 0x2222, Simhash: 0x1234}) {
		t.Error("a first fetch with no stored signal is not a change")
	}
}

// almostEqual is a small float helper for the closed-form checks.
func almostEqual(a, b, tol float64) bool { return math.Abs(a-b) <= tol }
