package freshness

import (
	"math"
	"testing"

	"github.com/tamnd/meguri"
)

// TestTauConvergesToBudget checks the water-level controller drives the total
// scheduled rate to the budget (doc 06, section 8). It builds a fixed population
// of mixed change rates, then ticks tau off the population's scheduled rate at
// the current level until the schedule fits the budget. tau moves slowly, so it
// should settle within a bounded number of ticks without overshooting.
func TestTauConvergesToBudget(t *testing.T) {
	p := DefaultParams()
	pop := make([]*meguri.URLRecord, 0, 200)
	for i := range 200 {
		// Log-spread rates from near-static to hyper-volatile, the shape the hump
		// acts on.
		lambda := math.Exp(-9+12*float64(i)/200) / 24 // wide band, changes/hour
		pop = append(pop, urlAt(lambda, 1))
	}

	scheduled := func(tau float64) float64 {
		var sum float64
		for _, u := range pop {
			sum += 1.0 / TargetInterval(u, nil, tau, p)
		}
		return sum
	}

	// Target half the rate a zero water level would schedule, so funding has to be
	// cut back and tau must rise to a real positive level.
	full := scheduled(0)
	budget := full / 2
	c := NewTauController(budget)

	for range 2000 {
		c.Tick(scheduled(c.Tau()))
		if math.Abs(scheduled(c.Tau())-budget)/budget < 0.05 {
			return // converged within the dead band
		}
	}
	t.Fatalf("tau did not converge: budget=%.4g final scheduled=%.4g tau=%.6g", budget, scheduled(c.Tau()), c.Tau())
}

// TestTauZeroBudgetIsInert checks a non-positive budget leaves tau untouched, so
// a partition with no budget pressure funds every URL to its own optimal rate.
func TestTauZeroBudgetIsInert(t *testing.T) {
	c := NewTauController(0)
	start := c.Tau()
	c.Tick(1e9) // a huge scheduled rate would normally raise tau
	if c.Tau() != start {
		t.Fatalf("zero-budget controller moved tau from %.6g to %.6g", start, c.Tau())
	}
}
