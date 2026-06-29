package prioritize

import (
	"math"

	"github.com/tamnd/meguri"
)

// OPIC is the online importance estimator of doc 09: Adaptive On-Line Page
// Importance Computation (Abiteboul, Preda, Cobena, WWW 2003). It models
// importance as cash flowing through the link graph. Every page holds an amount
// of cash, the importance it has accumulated but not yet distributed; when a
// page is crawled it absorbs its cash into a discounted history and splits the
// cash equally across its out-links, minus a teleport cut. A page's importance
// is read from its discounted history plus its currently held cash, the
// forward-looking estimate that lets an uncrawled page a hub points at be
// prioritized before it is ever fetched.
//
// The estimator stores, per URL, only the two numbers OPIC needs: the held cash
// and the discounted history. It never materializes the link matrix, which is
// the whole reason OPIC exists; each event is O(out-degree) and local. The
// durable importance is the URLRecord.Priority column the frontier sorts on
// (doc 03): cash and history are resident working state the prioritizer owns,
// and Priority is the served result recomputed from them. On recovery the
// working state rebuilds empty and re-converges from the live crawl, which OPIC
// is built to tolerate, because it is an online estimate that sharpens as cash
// flows, not a fixed point that must be restored exactly.
type OPIC struct {
	state    map[meguri.URLKey]*cashState
	teleport float32 // cash held by the virtual teleport node, redistributed lazily
	discount float32
	telRate  float32
	logNorm  float64
}

// cashState is one URL's OPIC working state: the cash it holds and the
// discounted history it has accumulated.
type cashState struct {
	cash    float32
	history float32
}

// NewOPIC returns an estimator configured from p.
func NewOPIC(p Params) *OPIC {
	ln := p.LogNorm
	if ln <= 0 {
		ln = 1
	}
	return &OPIC{
		state:    make(map[meguri.URLKey]*cashState),
		discount: p.Discount,
		telRate:  p.TeleportRate,
		logNorm:  ln,
	}
}

// Known reports how many URLs the estimator tracks, the denominator of the lazy
// teleport redistribution.
func (o *OPIC) Known() int { return len(o.state) }

func (o *OPIC) get(key meguri.URLKey) *cashState {
	st := o.state[key]
	if st == nil {
		st = &cashState{}
		o.state[key] = st
	}
	return st
}

// Seed injects an initial cash endowment for a URL the crawl starts from, the
// "fixed total cash distributed across the known pages" of the OPIC model. A
// seed enters with cash equal to its configured importance so the first crawl
// has flow to distribute; a discovered URL instead accrues its cash from the
// links that point at it (Credit).
func (o *OPIC) Seed(key meguri.URLKey, cash float32) {
	st := o.get(key)
	st.cash += cash
}

// Credit adds the cash a single in-link carries to the target URL's held cash
// (creditCash, doc 09). It is called on the receiving side for each discovered
// out-link after the seen-set check, whether the target is new or a rediscovery,
// so a URL many crawled pages point at accumulates cash fast and earns a high
// forward-looking importance before its own first crawl.
func (o *OPIC) Credit(key meguri.URLKey, weight float32) {
	if weight <= 0 {
		return
	}
	o.get(key).cash += weight
}

// Distribute implements one OPIC visit for a crawled source page (distributeCash,
// doc 09): the page absorbs its held cash into its discounted history, then
// splits the cash equally across its out-links after a teleport cut, writing
// each Discovery's LinkWeight so the cash travels with the routed link, and
// resets its own cash to zero. A page with no out-links is a dangling node: all
// its cash goes to teleport so it is never a sink that traps importance.
//
// The links slice is mutated in place: each LinkWeight is filled with the share
// this link carries, ready for the frontier to credit locally or route to the
// owning partition.
func (o *OPIC) Distribute(src meguri.URLKey, links []meguri.Discovery) {
	st := o.get(src)
	st.history = st.history*o.discount + st.cash

	if len(links) == 0 {
		o.teleport += st.cash
		st.cash = 0
		return
	}
	cut := st.cash * o.telRate
	o.teleport += cut
	share := (st.cash - cut) / float32(len(links))
	for i := range links {
		links[i].LinkWeight = share
	}
	st.cash = 0
}

// Score turns a URL's OPIC state into an importance estimate (opicScore, doc 09).
// It blends the discounted accumulated history (earned importance, dominant once
// a URL has been crawled many times) with the currently held cash (forward-
// looking importance, dominant before the first crawl), plus a uniform teleport
// floor so a URL no one links to is not exactly zero. The Log1p compresses the
// heavy tail, the same scaling tsumugi applies to PageRank, so one hub's cash
// does not swamp the priority range and the score spreads across the front-bank
// levels. The result lands in a stable range the front bank can quantize.
func (o *OPIC) Score(key meguri.URLKey) float32 {
	st := o.state[key]
	var cash, history float32
	if st != nil {
		cash, history = st.cash, st.history
	}
	var floor float32
	if n := len(o.state); n > 0 {
		floor = o.teleport / float32(n)
	}
	raw := history + cash + floor
	return float32(math.Log1p(float64(raw)) / o.logNorm)
}
