package prioritize

import (
	"math"
	"testing"

	"github.com/tamnd/meguri"
)

func almostEqual(a, b, tol float64) bool { return math.Abs(a-b) <= tol }

// TestBlendFallsBackToOPIC checks the first-crawl case: with nothing imported the
// blend returns the OPIC estimate unchanged, so the engine orders a fresh
// frontier on OPIC alone (doc 09, D10).
func TestBlendFallsBackToOPIC(t *testing.T) {
	p := DefaultParams()
	got := Blend(0.4, 0, false, 0, p)
	if !almostEqual(float64(got), 0.4, 1e-6) {
		t.Fatalf("blend without imports = %.6g, want the OPIC value 0.4", got)
	}
}

// TestBlendLeansOnPageRank checks a real imported per-page PageRank carries most
// of the weight but never crowds OPIC out entirely, so the order tracks links the
// stale import missed (doc 09).
func TestBlendLeansOnPageRank(t *testing.T) {
	p := DefaultParams()
	// Equal OPIC, but one URL has a strong imported PageRank.
	plain := Blend(0.3, 0, false, 0, p)
	ranked := Blend(0.3, 0.9, true, 0, p)
	if !(ranked > plain) {
		t.Fatalf("imported PageRank did not lift the priority: plain=%.6g ranked=%.6g", plain, ranked)
	}
	// OPIC keeps a quarter of the weight even with a page rank present.
	if p.WOPICWithPage <= 0 {
		t.Fatal("OPIC weight dropped to zero alongside a page rank, losing the live correction")
	}
}

// TestBlendHostOnlyShiftsOrder checks that with only a host score, OPIC orders
// within the host and the host score lifts or sinks the whole host.
func TestBlendHostOnlyShiftsOrder(t *testing.T) {
	p := DefaultParams()
	low := Blend(0.3, 0, false, Compress(0.1), p)
	high := Blend(0.3, 0, false, Compress(5.0), p)
	if !(high > low) {
		t.Fatalf("host score did not shift the order: low=%.6g high=%.6g", low, high)
	}
}

// TestUpdateHostBudgetTracksInDegree checks STAR: more distinct cross-host
// in-links buy more budget, bounded by the floor and the cap (doc 09).
func TestUpdateHostBudgetTracksInDegree(t *testing.T) {
	p := DefaultParams()
	var none, some, many meguri.HostRecord
	UpdateHostBudget(&none, 0, p)
	UpdateHostBudget(&some, 4, p)
	UpdateHostBudget(&many, 1_000_000, p)

	if none.URLBudget < p.MinBudget {
		t.Errorf("a host with no in-links fell below the floor: %d < %d", none.URLBudget, p.MinBudget)
	}
	if !(some.URLBudget > none.URLBudget) {
		t.Errorf("more in-degree did not raise the budget: none=%d some=%d", none.URLBudget, some.URLBudget)
	}
	if many.URLBudget != p.MaxBudget {
		t.Errorf("budget not capped: got %d want %d", many.URLBudget, p.MaxBudget)
	}
}

// TestCrossHostInDegreeIgnoresSameHost is the spam defense (doc 09): dense
// internal links add no reputation, only distinct other host groups do.
func TestCrossHostInDegreeIgnoresSameHost(t *testing.T) {
	c := NewCrossHostInDegree()
	const target = uint64(100)
	// A flood of same-host links.
	for range 1000 {
		c.Observe(target, target)
	}
	if c.Count(target) != 0 {
		t.Fatalf("same-host links inflated the cross-host in-degree to %d", c.Count(target))
	}
	// Two distinct other hosts.
	c.Observe(target, 200)
	c.Observe(target, 300)
	c.Observe(target, 200) // a repeat from a known source does not double-count
	if c.Count(target) != 2 {
		t.Fatalf("distinct cross-host in-degree = %d, want 2", c.Count(target))
	}
}

// TestAdmitDefersNotDiscards checks BEAST: an over-budget or too-deep discovery is
// parked in Trapped, not dropped (doc 09).
func TestAdmitDefersNotDiscards(t *testing.T) {
	h := &meguri.HostRecord{URLBudget: 2, URLCount: 2, DepthCap: 5}
	if got := Admit(h, 1); got != meguri.StatusTrapped {
		t.Errorf("over-budget URL not parked: got %v", got)
	}
	h2 := &meguri.HostRecord{URLBudget: 100, URLCount: 0, DepthCap: 5}
	if got := Admit(h2, 9); got != meguri.StatusTrapped {
		t.Errorf("too-deep URL not parked: got %v", got)
	}
	if got := Admit(h2, 1); got != meguri.StatusScheduled {
		t.Errorf("in-budget shallow URL not scheduled: got %v", got)
	}
}

// TestDepthPenaltyDecays checks the shallow-first tilt: deeper URLs are scaled
// down, the seed-depth URL untouched (doc 09).
func TestDepthPenaltyDecays(t *testing.T) {
	p := DefaultParams()
	if got := DepthPenalty(1.0, 0, p); got != 1.0 {
		t.Errorf("depth 0 changed the priority: %.6g", got)
	}
	shallow := DepthPenalty(1.0, 2, p)
	deep := DepthPenalty(1.0, 12, p)
	if !(shallow > deep) {
		t.Fatalf("depth penalty not monotone: shallow=%.6g deep=%.6g", shallow, deep)
	}
}

// TestTrapPenaltySinks checks a trap-suspect host's URLs are multiplied down, an
// unflagged host's left alone (doc 09).
func TestTrapPenaltySinks(t *testing.T) {
	p := DefaultParams()
	clean := &meguri.HostRecord{}
	trap := &meguri.HostRecord{Flags: meguri.HostFlagTrapSuspect}
	if got := TrapPenalty(1.0, clean, p); got != 1.0 {
		t.Errorf("clean host penalized: %.6g", got)
	}
	if got := TrapPenalty(1.0, trap, p); !almostEqual(float64(got), float64(p.TrapSuspectFactor), 1e-6) {
		t.Errorf("trap host not sunk to the factor: %.6g want %.6g", got, p.TrapSuspectFactor)
	}
}

// TestBudgetSplitHoldsRatio checks the deficit-round-robin holds the configured
// discovery-versus-refresh ratio over a long run when both streams have work.
func TestBudgetSplitHoldsRatio(t *testing.T) {
	p := DefaultParams()
	p.DiscoveryShare = 0.4
	p.RefreshShare = 0.6
	b := NewBudgetSplit(p)
	const n = 10000
	var disc int
	for range n {
		if b.Admit(true, true) == StreamDiscovery {
			disc++
		}
	}
	frac := float64(disc) / n
	if !almostEqual(frac, 0.4, 0.02) {
		t.Fatalf("realized discovery fraction %.3f, want ~0.40", frac)
	}
}

// TestBudgetSplitBorrowsIdle checks the share is proportional, not a hard
// partition: when discovery has no ready work the refresh stream takes every
// admit rather than letting the fetcher idle.
func TestBudgetSplitBorrowsIdle(t *testing.T) {
	b := NewBudgetSplit(DefaultParams())
	for i := range 100 {
		if b.Admit(false, true) != StreamRefresh {
			t.Fatalf("refresh did not borrow idle discovery capacity at step %d", i)
		}
	}
}

// TestQuantizeMonotone checks the front-bank mapping never sends a higher
// priority to a lower level (doc 09).
func TestQuantizeMonotone(t *testing.T) {
	const levels = 12
	prev := -1
	for _, pri := range []float32{0, 0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.95, 1.0} {
		lvl := Quantize(pri, levels)
		if lvl < prev {
			t.Fatalf("quantize not monotone at %.3g: level %d after %d", pri, lvl, prev)
		}
		prev = lvl
	}
}

// TestCrossesOnlyOnLevelChange checks the re-bucket trigger fires on a
// level-crossing credit and not on a sub-level nudge.
func TestCrossesOnlyOnLevelChange(t *testing.T) {
	const levels = 12
	if Crosses(0.30, 0.31, levels) {
		t.Error("a tiny credit wrongly reported a level crossing")
	}
	if !Crosses(0.05, 0.9, levels) {
		t.Error("a large credit did not report a level crossing")
	}
}
