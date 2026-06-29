package prioritize

// StreamKind names the two sources that feed the front bank: the discovery
// stream of new URLs awaiting a first crawl, and the refresh stream of due
// recrawls the rescheduler has marked ready.
type StreamKind uint8

const (
	StreamDiscovery StreamKind = iota // a new URL, growing coverage
	StreamRefresh                     // a due recrawl, maintaining freshness
)

// BudgetSplit holds the discovery-versus-refresh ratio at the front-bank feed
// (doc 09, section "The discovery-versus-refresh budget split"). It is the one
// knob that decides whether the crawler is growing or maintaining: a
// discovery-heavy split grows coverage, a refresh-heavy split keeps a known
// frontier fresh. The split is proportional, not a hard partition, so when one
// stream has no ready work the other takes the whole budget rather than letting
// the fetcher idle on a quota it cannot fill.
//
// admit is a deficit-round-robin: it serves whichever stream is furthest behind
// its share, which holds the long-run ratio at the configured split with two
// counters and no global accounting.
type BudgetSplit struct {
	discShare float64
	refrShare float64
	discAdm   float64
	refrAdm   float64
}

// NewBudgetSplit returns a split at the configured shares. Non-positive shares
// fall back to the params default so the ratio is always well formed.
func NewBudgetSplit(p Params) *BudgetSplit {
	d, r := p.DiscoveryShare, p.RefreshShare
	if d <= 0 {
		d = 0.5
	}
	if r <= 0 {
		r = 0.5
	}
	return &BudgetSplit{discShare: d, refrShare: r}
}

// Admit decides which stream to pull from next, given which streams currently
// have ready work, and charges the chosen stream. discoveryReady and
// refreshReady report whether each stream has an admissible URL right now. It
// holds the long-run ratio at the configured split while letting either stream
// borrow the other's idle capacity in the short run.
func (b *BudgetSplit) Admit(discoveryReady, refreshReady bool) StreamKind {
	discScore := b.discAdm / b.discShare
	refrScore := b.refrAdm / b.refrShare
	if discoveryReady && (discScore <= refrScore || !refreshReady) {
		b.discAdm++
		return StreamDiscovery
	}
	if refreshReady {
		b.refrAdm++
		return StreamRefresh
	}
	return StreamDiscovery // both empty: caller idles
}

// Shares returns the running counts admitted to each stream, the way a test or a
// monitor checks the realized ratio against the configured one.
func (b *BudgetSplit) Shares() (discovery, refresh float64) {
	return b.discAdm, b.refrAdm
}
