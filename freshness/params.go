package freshness

// Params are the rescheduler's tunable knobs (doc 06, section 12). The defaults
// are starting points for the benchmark sweep, not proven optimums: doc 14
// validates them against real crawl data before any are published as
// recommended (D19). Rates are in changes per hour, intervals in hours, to match
// the epoch-hours timestamps of the data model (doc 03).
type Params struct {
	// Estimator (section 3).
	Alpha           float64 // no-change-ratio smoothing pseudo-count, the tuned stand-in for the paper's small-sample constant
	MinRate         float64 // lambda floor, changes/hour: a never-changed page still gets a slow re-probe, never a rate of zero
	MaxRate         float64 // lambda ceiling, changes/hour: a cap for numeric stability, the optimizer starves above it anyway
	RecencyHalfLife float64 // no-change intervals over which a streak pulls lambda down toward the streak-implied rate

	// Longevity gate (section 5). The frontier applies the simhash gate when it
	// classifies a change; this is carried for completeness and the standalone
	// estimator path.
	SimhashNearDupThreshold int // Hamming distance separating cosmetic churn from meaningful change, the doc 08 k=3 point

	// Allocation (section 4, 8).
	RefRate     float64 // the partition's feasible reference crawl rate, changes/hour: the point the marginal freshness gain is measured at, the source of the hump
	ReprobeRate float64 // crawls/hour for starved URLs: a slow re-probe so a page coming back to life is eventually observed

	// Spacing (section 6).
	MinReprobeGap    uint32 // minimum hours before a rescheduled URL is due again, so next_due never lands in the past
	HostSpreadWindow uint32 // hours over which a host's due times are spread, breaking the thundering herd deterministically
}

const (
	hoursPerYear      = 365 * 24
	hoursPerFortnight = 14 * 24
)

// DefaultParams returns the section-12 defaults: a 0.5 smoothing pseudo-count, a
// yearly lambda floor and a four-per-hour ceiling, a recency half-life of eight
// intervals, the k=3 simhash threshold, a one-crawl-per-day reference rate, a
// fortnightly re-probe floor, and a one-hour minimum gap with a one-day host
// spread.
func DefaultParams() Params {
	return Params{
		Alpha:                   0.5,
		MinRate:                 1.0 / hoursPerYear,
		MaxRate:                 4.0,
		RecencyHalfLife:         8,
		SimhashNearDupThreshold: 3,
		RefRate:                 1.0 / 24.0,
		ReprobeRate:             1.0 / hoursPerFortnight,
		MinReprobeGap:           1,
		HostSpreadWindow:        24,
	}
}

// clampRate holds a rate inside the configured band (section 3). The floor keeps
// a never-changed page on a slow re-probe instead of scheduling it never; the
// ceiling is for numeric stability.
func clampRate(r, lo, hi float64) float64 {
	if r < lo {
		return lo
	}
	if r > hi {
		return hi
	}
	return r
}
