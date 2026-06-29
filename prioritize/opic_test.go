package prioritize

import (
	"testing"

	"github.com/tamnd/meguri"
)

// key builds a URLKey from a host and path id, enough to give tests distinct
// URLs on distinct or shared hosts.
func key(host, path uint64) meguri.URLKey {
	return meguri.URLKey{HostKey: host, PathKey: path}
}

// link builds a Discovery for a target from a source host, the unit the OPIC and
// STAR signals flow on.
func link(target meguri.URLKey, srcHost uint64) meguri.Discovery {
	return meguri.Discovery{URLKey: target, SrcHostKey: srcHost}
}

// TestOPICCashFlowsToInLinks is the central OPIC claim (doc 09): cash spreads
// from a crawled page to its out-links, so a page many crawled pages point at
// accumulates more cash, and a higher score, than a page few point at, even
// before either is itself crawled.
func TestOPICCashFlowsToInLinks(t *testing.T) {
	o := NewOPIC(DefaultParams())
	hub := key(9, 1)    // pointed at by three crawled pages
	lonely := key(9, 2) // pointed at by one

	// Three source pages, each seeded with cash, each linking to the hub.
	for s := uint64(10); s < 13; s++ {
		src := key(s, 1)
		o.Seed(src, 1)
		links := []meguri.Discovery{link(hub, s)}
		o.Distribute(src, links)
		o.Credit(links[0].URLKey, links[0].LinkWeight)
	}
	// One source page links to the lonely URL.
	src := key(20, 1)
	o.Seed(src, 1)
	links := []meguri.Discovery{link(lonely, 20)}
	o.Distribute(src, links)
	o.Credit(links[0].URLKey, links[0].LinkWeight)

	if !(o.Score(hub) > o.Score(lonely)) {
		t.Fatalf("hub did not outscore the lonely URL: hub=%.6g lonely=%.6g", o.Score(hub), o.Score(lonely))
	}
}

// TestOPICDanglingFeedsTeleport checks a crawled page with no out-links is not a
// sink: its cash goes to the teleport node, which then lifts every URL's floor,
// rather than vanishing or trapping importance.
func TestOPICDanglingFeedsTeleport(t *testing.T) {
	o := NewOPIC(DefaultParams())
	dangling := key(1, 1)
	other := key(2, 1)
	o.Seed(dangling, 4)
	o.Seed(other, 0) // known but holds nothing of its own

	before := o.Score(other)
	o.Distribute(dangling, nil) // no links: all cash to teleport
	after := o.Score(other)

	if !(after > before) {
		t.Fatalf("dangling cash did not reach the teleport floor: before=%.6g after=%.6g", before, after)
	}
}

// TestOPICCreditMonotone checks the forward-looking estimate rises with the cash
// an uncrawled URL's in-links have sent it.
func TestOPICCreditMonotone(t *testing.T) {
	o := NewOPIC(DefaultParams())
	k := key(1, 1)
	s0 := o.Score(k)
	o.Credit(k, 0.5)
	s1 := o.Score(k)
	o.Credit(k, 0.5)
	s2 := o.Score(k)
	if !(s1 > s0 && s2 > s1) {
		t.Fatalf("score not monotone in credited cash: %.6g %.6g %.6g", s0, s1, s2)
	}
}

// TestOPICHistoryAbsorbsCash checks a visit folds held cash into history and
// resets the cash, so a crawled hub keeps a high score from its earned history
// even after it has spent its cash onward.
func TestOPICHistoryAbsorbsCash(t *testing.T) {
	o := NewOPIC(DefaultParams())
	hub := key(1, 1)
	o.Seed(hub, 3)
	scoreWithCash := o.Score(hub)
	o.Distribute(hub, []meguri.Discovery{link(key(2, 1), 1)}) // spends cash, fills history
	scoreAfter := o.Score(hub)
	// History now carries what the cash was, so the visited hub keeps essentially
	// all of its importance rather than collapsing to zero once it spent its cash.
	if !(scoreAfter >= 0.9*scoreWithCash) {
		t.Fatalf("history did not retain the visited hub's importance: before=%.6g after=%.6g", scoreWithCash, scoreAfter)
	}
}
