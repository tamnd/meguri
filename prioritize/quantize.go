package prioritize

import "math"

// Quantize maps a continuous priority in roughly [0, 1) to one of numLevels
// front-bank buckets (doc 09, section "Quantization to priority levels"). The
// mapping is monotone, higher priority to a higher-or-equal level, and
// logarithmic, so the crowded low end where most URLs sit gets fewer levels and
// the sparse high end where the few important pages sit gets more resolution.
// This doc's contract to doc 05 is only that the mapping is monotone and the
// input range is stable; doc 05 owns the bank mechanics and may tune the exact
// spacing, which the frontier's own frontBucket does for the resident ring.
func Quantize(pri float32, numLevels int) int {
	if numLevels <= 1 {
		return 0
	}
	if pri <= 0 {
		return 0
	}
	if pri >= 1 {
		return numLevels - 1
	}
	// Logarithmic: the level falls as the priority halves, with the span of
	// halvings spread across the available levels so a 12-level bank and a 64-level
	// bank both keep the top of the range finely resolved without saturating. A
	// priority below the span's floor lands in level 0.
	const spanHalvings = 16.0
	scale := float64(numLevels-1) / spanHalvings
	level := numLevels - 1 - int(-math.Log2(float64(pri))*scale)
	if level < 0 {
		return 0
	}
	if level >= numLevels {
		return numLevels - 1
	}
	return level
}

// Crosses reports whether two priorities fall in different quantization levels,
// the test that decides when a credited URL must be re-bucketed in the front
// bank (doc 09, section "Re-bucketing when a signal updates"). Most cash credits
// move the float without crossing a level, so they update the stored priority
// without paying the re-bucket; only a credit large enough to change the level
// triggers the move.
func Crosses(oldPri, newPri float32, numLevels int) bool {
	return Quantize(oldPri, numLevels) != Quantize(newPri, numLevels)
}
