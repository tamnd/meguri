package prioritize

import (
	"math"

	"github.com/tamnd/meguri"
)

// TrapPenalty scales a URL's priority down when its host is a suspected trap
// (doc 09, section "Down-weighting HostFlagTrapSuspect"). It does not zero the
// priority, because a trap-suspect host may still hold real pages: it sinks the
// host's URLs below genuine work elsewhere so the bounded budget the host does
// get is spent on its likely-real URLs first. The flag is set by the dedup trap
// detector (doc 08); this is ordering reading it.
func TrapPenalty(pri float32, h *meguri.HostRecord, p Params) float32 {
	if h != nil && h.Flags&meguri.HostFlagTrapSuspect != 0 {
		return pri * p.TrapSuspectFactor
	}
	return pri
}

// DepthPenalty scales priority down as link depth from the nearest seed grows
// (doc 09, section "Depth penalties"). Important pages cluster near the seeds;
// deep pages are disproportionately the long tail and the generated combinatorial
// spaces. The decay is gentle, a small per-level factor, so a genuinely important
// deep page can still earn its way up on a strong OPIC cash signal. The hard cut
// is the depth cap (BEAST, doc 09), which parks URLs past the cap in Trapped;
// this is the soft tilt below the cap.
func DepthPenalty(pri float32, depth uint16, p Params) float32 {
	if depth == 0 {
		return pri
	}
	return pri * float32(math.Pow(float64(p.DepthDecay), float64(depth)))
}
