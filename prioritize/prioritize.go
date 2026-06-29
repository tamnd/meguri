package prioritize

import "github.com/tamnd/meguri"

// Prioritizer ties the OPIC estimator, the import blend, the cross-host in-degree
// reputation, and the spam penalties into the one importance the frontier reads.
// It is the policy object the frontier holds when prioritization is on.
type Prioritizer struct {
	p     Params
	opic  *OPIC
	indeg *CrossHostInDegree

	// importedPageRank is the sparse per-page PageRank side table tsumugi
	// delivers (doc 09): present only for URLs a prior crawl covered, joined at
	// scoring time, raw (compressed when blended). A URL with no entry falls back
	// to its host score plus OPIC.
	importedPageRank map[meguri.URLKey]float32
}

// New returns a prioritizer configured from p.
func New(p Params) *Prioritizer {
	return &Prioritizer{
		p:                p,
		opic:             NewOPIC(p),
		indeg:            NewCrossHostInDegree(),
		importedPageRank: make(map[meguri.URLKey]float32),
	}
}

// Params returns the policy this prioritizer runs.
func (pr *Prioritizer) Params() Params { return pr.p }

// SeedCash gives a seed URL its initial OPIC endowment, the starting cash the
// first crawl distributes (doc 09's "fixed total cash across the known pages").
func (pr *Prioritizer) SeedCash(key meguri.URLKey, cash float32) {
	pr.opic.Seed(key, cash)
}

// Credit folds one discovered out-link into the importance signals: it adds the
// link's OPIC cash to the target URL's held cash and, when the link crosses host
// groups, counts it toward the target host's cross-host in-degree. It returns the
// target host's new distinct cross-host in-degree so the caller can refresh the
// host's url_budget (UpdateHostBudget). A same-host link credits cash but adds no
// reputation, the spam defense that stops dense internal links inflating a
// budget.
func (pr *Prioritizer) Credit(d meguri.Discovery) uint32 {
	pr.opic.Credit(d.URLKey, d.LinkWeight)
	return pr.indeg.Observe(d.URLKey.HostKey, d.SrcHostKey)
}

// Distribute spreads a just-crawled source page's accumulated cash equally
// across its out-links, filling each Discovery's LinkWeight, and folds the cash
// into the source's discounted history (one OPIC visit). The frontier then
// credits the local links and routes the rest.
func (pr *Prioritizer) Distribute(src meguri.URLKey, links []meguri.Discovery) {
	pr.opic.Distribute(src, links)
}

// ImportPageRank loads a per-page PageRank tsumugi computed over a prior crawl.
// It overwrites rather than accumulates, because a newer computation supersedes
// an older one (doc 09): the import is a refresh, not a sum.
func (pr *Prioritizer) ImportPageRank(key meguri.URLKey, rank float32) {
	pr.importedPageRank[key] = rank
}

// CrossHostInDegree returns the distinct cross-host in-degree recorded for a
// host, the STAR reputation that sets its budget.
func (pr *Prioritizer) CrossHostInDegree(hostKey uint64) uint32 {
	return pr.indeg.Count(hostKey)
}

// Score returns a URL's current OPIC importance estimate, the held cash and
// discounted history blended into a stable range (doc 09). It reads the
// accumulated signal without the import or penalty blend Priority applies, so a
// caller can confirm a routed link's cash actually landed on its target.
func (pr *Prioritizer) Score(key meguri.URLKey) float32 {
	return pr.opic.Score(key)
}

// Priority computes the final importance for a URL: the OPIC estimate, blended
// with any imported per-page PageRank and per-host quality, then scaled down by
// the trap-suspect and depth penalties (doc 09). It reads the record and its
// host but writes nothing; the caller stores the result in URLRecord.Priority and
// decides whether the change crosses a front-bank level.
func (pr *Prioritizer) Priority(rec *meguri.URLRecord, h *meguri.HostRecord) float32 {
	opic := pr.opic.Score(rec.URLKey)

	rank, havePage := pr.importedPageRank[rec.URLKey]
	var host float32
	if h != nil {
		host = Compress(h.HostScore)
	}
	pri := Blend(opic, Compress(rank), havePage, host, pr.p)
	pri = TrapPenalty(pri, h, pr.p)
	pri = DepthPenalty(pri, rec.Depth, pr.p)
	return pri
}
