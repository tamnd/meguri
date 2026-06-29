package freshness

// TauController maintains the global water level tau for a partition (section 8).
// tau is the dual price of a refresh crawl, one number per partition, read by
// every per-URL reschedule and adjusted on a slow background cadence so the total
// scheduled refresh rate tracks the budget B. It moves slowly because it is set
// by the aggregate of a whole partition and no single crawl moves the aggregate
// much, which is what lets the rescheduler stay incremental instead of running a
// stop-the-world batch (section 8, "Why meguri re-solves incrementally").
//
// This is the Edwards et al closed loop made incremental: the estimates and the
// schedule co-evolve through a slowly-moving tau, never a global re-solve.
type TauController struct {
	tau    float64 // current water level, read by every reschedule
	budget float64 // B, refresh crawls per period for this partition
	gain   float64 // multiplicative step per tick, small so tau does not overshoot
}

// NewTauController returns a controller targeting budget crawls per period,
// seeded at a small positive water level so the first ticks have something to
// scale. A non-positive budget yields a controller that funds nothing.
func NewTauController(budget float64) *TauController {
	return &TauController{
		tau:    1e-6,
		budget: budget,
		gain:   0.05,
	}
}

// Tau returns the current water level, the value a reschedule thresholds value
// densities against. It is safe to read between ticks; the controller only ever
// nudges it by a small step.
func (c *TauController) Tau() float64 { return c.tau }

// Tick folds the partition's current total scheduled crawl rate into the water
// level (section 8). Too much scheduled rate (over budget) raises tau, starving
// more pages; too little lowers it, funding more. The step is multiplicative and
// small, because tau moves slowly and overshoot wastes budget. A dead band around
// the budget keeps tau still when the schedule already fits, so it does not
// hunt. The controller is run on a slow cadence, not on the crawl hot path.
func (c *TauController) Tick(scheduledRate float64) {
	if c.budget <= 0 {
		return
	}
	switch {
	case scheduledRate > c.budget*1.02:
		c.tau *= 1 + c.gain // raise the level, fund fewer
	case scheduledRate < c.budget*0.98:
		c.tau *= 1 - c.gain // lower the level, fund more
	}
	if c.tau < 1e-12 {
		c.tau = 1e-12 // never collapse to zero, which would fund everything at once
	}
}
