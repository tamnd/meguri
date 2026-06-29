package distribute

import "testing"

// TestControlEpochBumps checks every map change advances the epoch, the single
// number a heartbeat compares to decide whether to pull a new map.
func TestControlEpochBumps(t *testing.T) {
	c := NewControl()
	e0 := c.Epoch()
	c.AddPartition("a")
	c.AddPartition("b")
	c.Pin(99, 1)
	c.SetHealth(0, Degraded)
	if c.Epoch() <= e0 {
		t.Fatalf("epoch did not advance: %d -> %d", e0, c.Epoch())
	}
	if c.FetchMap().NumPartitions != 3 {
		t.Fatalf("expected 3 partitions, got %d", c.FetchMap().NumPartitions)
	}
}

// TestControlAddMovesMinimal checks that adding a partition only pulls hosts onto
// the new one and never moves a host between the existing partitions, the
// minimal-movement guarantee the control plane inherits from jump hashing.
func TestControlAddMovesMinimal(t *testing.T) {
	c := NewControl()
	c.AddPartition("a")
	c.AddPartition("b") // now 3 partitions: 0,1,2
	before := c.FetchMap()
	newID := c.AddPartition("c") // now 4
	after := c.FetchMap()

	moved := 0
	for k := range uint64(20000) {
		hk := splitmix(k)
		b := before.Owner(hk)
		a := after.Owner(hk)
		if b != a {
			moved++
			if a != newID {
				t.Fatalf("host %d moved from %d to %d, not onto the new partition %d", hk, b, a, newID)
			}
		}
	}
	if moved == 0 {
		t.Fatal("adding a partition moved nothing")
	}
}

// TestControlRemoveHighEnd checks a remove drops the highest partition and
// refuses to go below one, the only shrink jump hashing supports cheaply.
func TestControlRemoveHighEnd(t *testing.T) {
	c := NewControl()
	c.AddPartition("a")
	id, ok := c.RemovePartition()
	if !ok || id != 1 {
		t.Fatalf("remove returned (%d,%v), want (1,true)", id, ok)
	}
	if _, ok := c.RemovePartition(); ok {
		t.Fatal("removed the last partition")
	}
}

// TestControlReplicaPlacement checks that with machines and a replica factor the
// control plane fills each partition's replica set, and that every replica is a
// distinct real machine.
func TestControlReplicaPlacement(t *testing.T) {
	c := NewControl()
	c.AddPartition("a")
	c.AddPartition("b")
	c.SetMachines([]Machine{{ID: 1, Weight: 1}, {ID: 2, Weight: 1}, {ID: 3, Weight: 1}, {ID: 4, Weight: 1}})
	c.SetReplicas(2)
	for _, p := range c.FetchMap().Partitions {
		if len(p.Replicas) != 2 {
			t.Fatalf("partition %d has %d replicas, want 2", p.ID, len(p.Replicas))
		}
		if p.Replicas[0] == p.Replicas[1] {
			t.Fatalf("partition %d has duplicate replicas %v", p.ID, p.Replicas)
		}
	}
}

// TestControlFetchMapIsSnapshot checks a fetched map does not change under a
// later control-plane edit, the property that lets a router cache it safely.
func TestControlFetchMapIsSnapshot(t *testing.T) {
	c := NewControl()
	snap := c.FetchMap()
	c.AddPartition("a")
	if snap.NumPartitions != 1 {
		t.Fatalf("snapshot mutated after a later edit: NumPartitions=%d", snap.NumPartitions)
	}
}
