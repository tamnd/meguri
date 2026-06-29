package distribute

import (
	"os"
	"testing"

	m "github.com/tamnd/meguri"
)

// Control is the live BacklogSource: the elasticity loop reads the backlog the
// partitions report on their heartbeats straight off the control plane.
var _ BacklogSource = (*Control)(nil)

// TestControlBacklogFromHeartbeats checks the heartbeat-fed backlog binding in
// isolation: a partition's reported depth lands in Control.Backlog at its id, a
// partition that has not beat reads zero, a removed partition's stale report is
// dropped, and the loaded beat still does the liveness restore a plain beat does.
func TestControlBacklogFromHeartbeats(t *testing.T) {
	ctl := NewControl()
	ctl.AddPartition("") // 2 partitions: ids 0 and 1

	ctl.HeartbeatLoad(0, 700)
	// Partition 1 has not beat yet.
	if b := ctl.Backlog(); len(b) != 2 || b[0] != 700 || b[1] != 0 {
		t.Fatalf("backlog = %v, want [700 0]", b)
	}

	ctl.HeartbeatLoad(1, 300)
	if b := ctl.Backlog(); b[1] != 300 {
		t.Fatalf("partition 1 backlog = %d, want 300", b[1])
	}

	// A loaded beat restores a downed partition the way a plain beat does.
	ctl.MissHeartbeat(0)
	ctl.MissHeartbeat(0)
	ctl.MissHeartbeat(0) // Failed at the default threshold
	if got := ctl.healthOf(0); got != Failed {
		t.Fatalf("partition 0 health = %v, want Failed before the loaded beat", got)
	}
	ctl.HeartbeatLoad(0, 50)
	if got := ctl.healthOf(0); got != Alive {
		t.Fatalf("partition 0 health = %v, want Alive after the loaded beat", got)
	}

	// Removing the high partition drops its stale backlog, so the slice tracks the
	// count and a later re-add starts clean.
	if _, ok := ctl.RemovePartition(); !ok {
		t.Fatal("remove partition")
	}
	if b := ctl.Backlog(); len(b) != 1 {
		t.Fatalf("backlog length = %d, want 1 after removal", len(b))
	}
	ctl.AddPartition("")
	if b := ctl.Backlog(); b[1] != 0 {
		t.Fatalf("re-added partition 1 backlog = %d, want 0 (stale report dropped)", b[1])
	}
}

// healthOf reads a partition's recorded health, a test helper over the map the
// control plane keeps.
func (c *Control) healthOf(id PartitionID) HealthState {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.m.Partitions {
		if p.ID == id {
			return p.Health
		}
	}
	return Failed
}

// TestElasticityScalesOnHeartbeatBacklog is the live binding on real data: every
// partition reports its jump-hash share of the frozen corpus as a heartbeat
// backlog into the control plane, the elasticity loop reads that backlog straight
// off Control (the BacklogSource is the control plane, not a hand-fed slice), and
// the fleet grows until the mean per-partition depth falls into band. This is the
// in-process double for the fleet heartbeat path: the routing, the per-partition
// depth, the beat, the aggregate, and the scale decision all run end to end on the
// 142083 real keys; only the wire the beat would cross is replaced by the call.
func TestElasticityScalesOnHeartbeatBacklog(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	part := loadCorpusKeys(t, path)
	keys := make([]m.URLKey, len(part.URLs))
	for i := range part.URLs {
		keys[i] = part.URLs[i].URLKey
	}
	total := len(keys)
	if total < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000 to spread a fleet", total)
	}

	// beat is the in-process heartbeat round: it routes every key to its owner under
	// the current map, then each partition beats its pending depth into the control
	// plane. The depth is the real jump-hash distribution, not an even spread, so the
	// loop scales on a true per-partition signal.
	beat := func(ctl *Control) {
		n := ctl.NumPartitions()
		mp := &Map{Epoch: 1, NumPartitions: n}
		depth := make([]int, n)
		for _, k := range keys {
			depth[mp.Owner(k.HostKey)]++
		}
		for id := range n {
			ctl.HeartbeatLoad(PartitionID(id), depth[id])
		}
	}

	const highWater = 5000
	ctl := NewControl()
	e := NewElasticity(ctl, ctl, ElasticityConfig{HighWater: highWater, MaxPartitions: 256, Breaches: 1})

	beat(ctl)
	ups := 0
	for range 256 {
		got := e.Tick()
		if got == ScaleUp {
			ups++
		}
		beat(ctl) // the partitions re-report under the new partition count
		if got == ScaleHold {
			break
		}
	}

	n := ctl.NumPartitions()
	// The loop scales on the mean across the real per-partition depths; once it
	// holds, that mean must be in band.
	bl := ctl.Backlog()
	sum := 0
	for _, d := range bl {
		sum += d
	}
	mean := sum / len(bl)
	if mean > highWater {
		t.Fatalf("loop held at %d partitions with mean heartbeat backlog %d over the %d water mark", n, mean, highWater)
	}
	if ups == 0 {
		t.Fatal("loop never scaled up off the heartbeat backlog")
	}
	// The signal is the real jump-hash distribution, so the per-partition depths are
	// not all equal: confirm the binding carried a genuine spread, not a flat fill.
	uneven := false
	for _, d := range bl {
		if d != bl[0] {
			uneven = true
			break
		}
	}
	if !uneven && n > 1 {
		t.Fatal("heartbeat backlog was perfectly even; expected the real jump-hash spread")
	}
	t.Logf("converged to %d partitions for %d urls at water mark %d (%d scale-ups), mean heartbeat backlog %d",
		n, total, highWater, ups, mean)
}
