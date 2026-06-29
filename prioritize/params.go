package prioritize

// Params is the prioritizer's tunable policy, the M5 counterpart to
// freshness.Params (doc 09). Every knob the OPIC estimator, the import blend, the
// STAR budget, the spam penalties, and the discovery-versus-refresh split read
// lives here, so a campaign tunes ordering without touching code. The defaults
// are the doc 09 targets, stated as starting points until the benchmark gate
// (D19) retunes them on real crawl data.
type Params struct {
	// OPIC online importance (doc 09, section "OPIC").
	Discount     float32 // history decay per visit, gamma slightly below 1
	TeleportRate float32 // fraction of distributed cash sent to the teleport node
	LogNorm      float64 // running normalizer keeping the OPIC score in a stable range

	// Import blend weights (doc 09, section "The blend function").
	WPageRank     float32 // weight on imported per-page PageRank, when present
	WOPICWithPage float32 // weight on OPIC when a per-page PageRank is present
	WHost         float32 // weight on host score when a per-page PageRank is present
	WOPICWithHost float32 // weight on OPIC when only a host score is present
	WHostOnly     float32 // weight on host score when only a host score is present

	// STAR per-host budget by cross-host in-degree (doc 09, section "Budget by
	// cross-host in-degree").
	BaseBudget uint32 // budget floor before any cross-host in-links
	PerInLink  uint32 // budget added per distinct cross-host in-link
	MinBudget  uint32 // a brand-new host still gets this, to be discovered at all
	MaxBudget  uint32 // no single host can claim more than this

	// Spam and trap dampeners (doc 09, section "Spam and trap avoidance").
	TrapSuspectFactor float32 // priority multiplier for a trap-suspect host, e.g. 0.1
	DepthDecay        float32 // per-level priority multiplier, e.g. 0.95

	// Discovery-versus-refresh budget split (doc 09, section "The one knob"). The
	// shares are a ratio; admit holds the long-run split between them.
	DiscoveryShare float64 // fraction of fetches spent growing the frontier
	RefreshShare   float64 // fraction of fetches spent keeping known URLs fresh
}

// DefaultParams returns the doc 09 starting policy.
//
// The discount is set for a multi-thousand-crawl half-life, so a page that was
// important once does not dominate the order forever but a stable hub stays high
// across a long campaign. The teleport rate is the standard 0.15 PageRank
// damping complement, so 85 percent of a page's cash follows its links and 15
// percent teleports, the same split tsumugi's graph signals use. The blend
// leans hardest on a real imported PageRank when one exists, keeps a quarter of
// the weight on OPIC to track links the stale import never saw, and falls all
// the way back to OPIC alone on the first crawl. The STAR budget is a floor plus
// a per-cross-host-in-link increment, clamped, so reputation an adversary cannot
// forge bounds the crawl an adversary can demand. The split starts
// discovery-heavy, the annealed-campaign default.
func DefaultParams() Params {
	return Params{
		Discount:     0.9998, // half-life ~3466 visits: ln(2)/ln(1/0.9998)
		TeleportRate: 0.15,
		LogNorm:      1.0,

		WPageRank:     0.6,
		WOPICWithPage: 0.25,
		WHost:         0.15,
		WOPICWithHost: 0.7,
		WHostOnly:     0.3,

		BaseBudget: 64,
		PerInLink:  16,
		MinBudget:  16,
		MaxBudget:  1 << 20,

		TrapSuspectFactor: 0.1,
		DepthDecay:        0.95,

		DiscoveryShare: 0.8,
		RefreshShare:   0.2,
	}
}
