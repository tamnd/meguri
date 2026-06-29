package distribute

import (
	"os"
	"testing"
)

// fixedBacklog is the in-process backlog source: a slice the test mutates between
// ticks to drive the loop. Its length is the partition count the loop sees, which
// the test keeps in step with the control plane.
type fixedBacklog struct{ depth []int }

func (f *fixedBacklog) Backlog() []int { return f.depth }

// spread sets every partition to the same depth across n partitions, the even-load
// case the mean-backlog signal scales on.
func (f *fixedBacklog) spread(n, depth int) {
	f.depth = make([]int, n)
	for i := range f.depth {
		f.depth[i] = depth
	}
}

// TestScaleUpAfterBreaches checks the loop holds while a high-water breach is
// still building and adds exactly one partition on the tick the breach count is
// met, the hysteresis that keeps a momentary spike from resizing the fleet.
func TestScaleUpAfterBreaches(t *testing.T) {
	ctl := NewControl()
	bl := &fixedBacklog{}
	bl.spread(1, 500)
	e := NewElasticity(ctl, bl, ElasticityConfig{HighWater: 100, MaxPartitions: 8, Breaches: 3})

	if got := e.Tick(); got != ScaleHold {
		t.Fatalf("tick 1 = %v, want Hold (breach building)", got)
	}
	if got := e.Tick(); got != ScaleHold {
		t.Fatalf("tick 2 = %v, want Hold (breach building)", got)
	}
	if got := e.Tick(); got != ScaleUp {
		t.Fatalf("tick 3 = %v, want Up (breach met)", got)
	}
	if got := ctl.NumPartitions(); got != 2 {
		t.Fatalf("partitions after scale-up = %d, want 2", got)
	}
}

// TestCooldownPreventsFlapping checks that after a resize the loop holds for the
// cooldown window even while the load stays breached, so one swing never cascades
// into a run of resizes.
func TestCooldownPreventsFlapping(t *testing.T) {
	ctl := NewControl()
	bl := &fixedBacklog{}
	bl.spread(1, 500)
	e := NewElasticity(ctl, bl, ElasticityConfig{HighWater: 100, MaxPartitions: 8, Breaches: 1, Cooldown: 2})

	if got := e.Tick(); got != ScaleUp {
		t.Fatalf("first tick = %v, want Up", got)
	}
	for i := range 2 {
		if got := e.Tick(); got != ScaleHold {
			t.Fatalf("cooldown tick %d = %v, want Hold", i, got)
		}
	}
	// Cooldown elapsed; a still-breached load may act again.
	if got := e.Tick(); got != ScaleUp {
		t.Fatalf("post-cooldown tick = %v, want Up", got)
	}
	if got := ctl.NumPartitions(); got != 3 {
		t.Fatalf("partitions = %d, want 3 (two scale-ups around one cooldown)", got)
	}
}

// TestScaleDownReturnsCapacity checks a sustained low-water load removes a
// partition, but never below MinPartitions.
func TestScaleDownReturnsCapacity(t *testing.T) {
	ctl := NewControl()
	ctl.AddPartition("") // start at 2 partitions
	bl := &fixedBacklog{}
	bl.spread(2, 1)
	e := NewElasticity(ctl, bl, ElasticityConfig{HighWater: 100, LowWater: 10, MinPartitions: 1, MaxPartitions: 8, Breaches: 2})

	if got := e.Tick(); got != ScaleHold {
		t.Fatalf("tick 1 = %v, want Hold (low breach building)", got)
	}
	if got := e.Tick(); got != ScaleDown {
		t.Fatalf("tick 2 = %v, want Down", got)
	}
	if got := ctl.NumPartitions(); got != 1 {
		t.Fatalf("partitions after scale-down = %d, want 1", got)
	}
	// At the floor, a further low-water run holds rather than removing the last
	// partition.
	bl.spread(1, 1)
	for i := range 4 {
		if got := e.Tick(); got != ScaleHold {
			t.Fatalf("floor tick %d = %v, want Hold (at MinPartitions)", i, got)
		}
	}
}

// TestHoldInBand checks a load between the water marks holds and resets the
// streaks, so a fleet that crosses back into band does not carry stale breach
// momentum into a later resize.
func TestHoldInBand(t *testing.T) {
	ctl := NewControl()
	bl := &fixedBacklog{}
	e := NewElasticity(ctl, bl, ElasticityConfig{HighWater: 100, LowWater: 10, MaxPartitions: 8, Breaches: 2})

	bl.spread(1, 500) // breach high once
	if got := e.Tick(); got != ScaleHold {
		t.Fatalf("tick 1 = %v, want Hold", got)
	}
	bl.spread(1, 50) // back in band before the breach completed
	if got := e.Tick(); got != ScaleHold {
		t.Fatalf("in-band tick = %v, want Hold", got)
	}
	if e.highStreak != 0 {
		t.Fatalf("in-band tick left highStreak = %d, want 0 (reset)", e.highStreak)
	}
}

// TestScaleUpRespectsMax checks the loop never grows past MaxPartitions however
// long the load stays breached.
func TestScaleUpRespectsMax(t *testing.T) {
	ctl := NewControl()
	bl := &fixedBacklog{}
	bl.spread(1, 9999)
	e := NewElasticity(ctl, bl, ElasticityConfig{HighWater: 1, MaxPartitions: 3, Breaches: 1})
	for range 20 {
		bl.spread(ctl.NumPartitions(), 9999)
		e.Tick()
	}
	if got := ctl.NumPartitions(); got != 3 {
		t.Fatalf("partitions = %d, want the cap of 3", got)
	}
}

// TestElasticityGrowsToBandOnCorpus drives the loop with a backlog seeded from the
// real corpus URL count and checks it grows the fleet until the mean per-partition
// backlog falls into band, the steady state an autoscaler converges to, then holds.
// It uses the real corpus size as the load so the convergence target is a real
// number, not a synthetic one. It skips when no corpus is configured.
func TestElasticityGrowsToBandOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	src := loadCorpusKeys(t, path)
	total := len(src.URLs)
	if total < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000 to spread a fleet", total)
	}

	const highWater = 5000
	ctl := NewControl()
	bl := &fixedBacklog{}
	// The backlog source models the whole corpus spread evenly over however many
	// partitions the loop has grown to, so the mean depth is total/n.
	refresh := func() { bl.spread(ctl.NumPartitions(), total/ctl.NumPartitions()) }
	refresh()
	e := NewElasticity(ctl, bl, ElasticityConfig{HighWater: highWater, MaxPartitions: 256, Breaches: 1})

	ups := 0
	for range 256 {
		got := e.Tick()
		if got == ScaleUp {
			ups++
		}
		refresh()
		if got == ScaleHold {
			break
		}
	}

	n := ctl.NumPartitions()
	if mean := total / n; mean > highWater {
		t.Fatalf("loop stopped at %d partitions with mean backlog %d still over the %d water mark", n, mean, highWater)
	}
	// One partition past the band would already have been under water, so the loop
	// must not have overshot by more than its last step.
	if mean := total / (n - 1); n > 1 && mean <= highWater {
		t.Fatalf("loop overshot: %d partitions, but %d already sat in band", n, n-1)
	}
	t.Logf("converged to %d partitions for %d urls at water mark %d (%d scale-ups)", n, total, highWater, ups)
}
