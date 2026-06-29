package prioritize

import "math"

// Compress puts an imported host quality or per-page PageRank score on the same
// log-compressed scale as the OPIC estimate, so the blend adds comparable
// quantities. A zero score (no import) stays zero, contributing nothing. It is
// the same Log1p heavy-tail compression OPIC's own score applies, the scaling
// tsumugi uses before serving a PageRank.
func Compress(score float32) float32 {
	if score <= 0 {
		return 0
	}
	return float32(math.Log1p(float64(score)))
}

// Blend combines the online OPIC estimate with any imported signals into the
// importance written to URLRecord.Priority (blendedPriority, doc 09). OPIC is
// always present. The imported per-page PageRank is present only for a URL a
// prior tsumugi crawl covered; havePage says whether pageRank is real. The
// imported per-host quality is present for a host tsumugi scored; a zero
// hostScore means none.
//
// The weights shift toward the import when it exists, because a real PageRank
// over a real crawl is better evidence than an online estimate, but OPIC never
// drops to zero even alongside a per-page rank: the import is already stale by
// the time it lands and OPIC sees the links the prior crawl missed, so a quarter
// of the weight stays on the live signal. When nothing was imported OPIC is the
// whole answer, the first-crawl case that lets the engine order a frontier with
// no global graph at all.
//
// pageRank and hostScore are expected already log-compressed onto the OPIC
// scale (Compress for both imported signals).
func Blend(opic, pageRank float32, havePage bool, hostScore float32, p Params) float32 {
	switch {
	case havePage:
		return p.WPageRank*pageRank + p.WOPICWithPage*opic + p.WHost*hostScore
	case hostScore != 0:
		return p.WOPICWithHost*opic + p.WHostOnly*hostScore
	default:
		return opic
	}
}
