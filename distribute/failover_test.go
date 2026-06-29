package distribute

import (
	"os"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

func TestHeartbeatHealthTransitions(t *testing.T) {
	c := NewControl()
	id := PartitionID(0)

	if got := c.MissHeartbeat(id); got != Degraded {
		t.Fatalf("first miss -> %v, want Degraded", got)
	}
	// The default threshold is 3, so the third consecutive miss fails it.
	c.MissHeartbeat(id)
	if got := c.MissHeartbeat(id); got != Failed {
		t.Fatalf("third miss -> %v, want Failed", got)
	}
	failedEpoch := c.Epoch()

	// A live beat clears the misses and restores the partition, bumping the epoch.
	c.Heartbeat(id)
	if h := mapHealth(c, id); h != Alive {
		t.Fatalf("after a heartbeat health = %v, want Alive", h)
	}
	if c.Epoch() <= failedEpoch {
		t.Fatal("restoring a failed partition did not bump the epoch")
	}

	// Having cleared, the next miss starts over at Degraded, not Failed.
	if got := c.MissHeartbeat(id); got != Degraded {
		t.Fatalf("miss after recovery -> %v, want Degraded (counter reset)", got)
	}
}

func TestFailMachinePromotesReplica(t *testing.T) {
	c := NewControl()
	for range 7 {
		c.AddPartition("")
	}
	c.SetReplicas(2)
	machines := []Machine{
		{ID: 1, Weight: 1, Address: "m1:7000"},
		{ID: 2, Weight: 1, Address: "m2:7000"},
		{ID: 3, Weight: 1, Address: "m3:7000"},
		{ID: 4, Weight: 1, Address: "m4:7000"},
	}
	c.SetMachines(machines)

	before := c.FetchMap()
	// Pick a machine that is primary for at least one partition.
	var victim MachineID = 2
	primariesOf := map[MachineID]int{}
	for _, p := range before.Partitions {
		primariesOf[p.Primary]++
	}
	if primariesOf[victim] == 0 {
		t.Skipf("machine %d holds no primary in this placement", victim)
	}
	epochBefore := before.Epoch

	promoted := c.FailMachine(victim)
	if len(promoted) == 0 {
		t.Fatal("failing a machine that held primaries promoted nothing")
	}
	after := c.FetchMap()
	if after.Epoch <= epochBefore {
		t.Fatal("FailMachine did not bump the epoch")
	}

	// The failed machine must own nothing now, and every promoted partition must
	// land on the next surviving machine in its rendezvous preference list.
	survivors := []MachineID{1, 3, 4}
	for _, p := range after.Partitions {
		if p.Primary == victim {
			t.Fatalf("partition %d still primaried on the failed machine", p.ID)
		}
		for _, rep := range p.Replicas {
			if MachineID(rep) == victim {
				t.Fatalf("partition %d still lists the failed machine as a replica", p.ID)
			}
		}
	}
	for _, pid := range promoted {
		want := preferenceList(pid, survivors, 2)
		got := mapPrimary(after, pid)
		if len(want) == 0 || got != want[0] {
			t.Fatalf("partition %d promoted to %d, want the top survivor %v", pid, got, want)
		}
		// The routing address must follow the new primary.
		if addr := mapAddress(after, pid); addr != addressOfMachine(machines, got) {
			t.Fatalf("partition %d address = %q, want the new primary's %q", pid, addr, addressOfMachine(machines, got))
		}
	}

	// Re-failing the same machine is a no-op: it is already gone.
	if again := c.FailMachine(victim); again != nil {
		t.Fatalf("re-failing an absent machine promoted %v, want nothing", again)
	}
}

// TestFailoverOnCorpus is the M9 failover gate on real data: build a primary
// partition from the frozen ccrawl slice with some URLs in flight, replicate it
// to a rendezvous-placed standby, lose the primary's machine, and promote the
// standby. The promoted partition must carry every host and URL the primary held,
// reset the in-flight work conservatively to Scheduled, and write a well-formed
// .meguri file (doc 12, sections 4 and 5).
func TestFailoverOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	src := loadCorpusKeys(t, path)
	if len(src.URLs) < 100 {
		t.Skipf("corpus has %d urls, need at least 100", len(src.URLs))
	}

	// Mark every eleventh URL in flight, the work a crash leaves uncertain.
	inFlight := 0
	for i := range src.URLs {
		if i%11 == 0 {
			src.URLs[i].Status = m.StatusInFlight
			inFlight++
		}
	}
	if inFlight == 0 {
		t.Skip("no urls to mark in flight")
	}

	// The standby loads the primary's snapshot and is caught up to it.
	const snapLSN = 5000
	replica := NewReplica(src, snapLSN)
	if replica.Lag(snapLSN) != 0 {
		t.Fatal("a freshly shipped replica should have zero lag at the snapshot LSN")
	}

	// The control plane loses the primary's machine and promotes the standby.
	promoted := replica.Promote()

	if len(promoted.URLs) != len(src.URLs) {
		t.Fatalf("promoted partition has %d urls, primary had %d", len(promoted.URLs), len(src.URLs))
	}
	if len(promoted.Hosts) != len(src.Hosts) {
		t.Fatalf("promoted partition has %d hosts, primary had %d", len(promoted.Hosts), len(src.Hosts))
	}
	for _, u := range promoted.URLs {
		if u.Status == m.StatusInFlight {
			t.Fatal("an in-flight URL survived promotion, it must reset to Scheduled")
		}
	}
	if _, err := format.Decode(mustEncode(t, promoted)); err != nil {
		t.Fatalf("promoted partition is not a well-formed .meguri file: %v", err)
	}

	t.Logf("promoted a replica of %d urls across %d hosts, recovered %d in-flight urls to Scheduled, .meguri round-trips",
		len(promoted.URLs), len(promoted.Hosts), inFlight)
}

// mapHealth, mapPrimary, mapAddress read a partition's fields out of a fetched
// map for the assertions above.
func mapHealth(c *Control, id PartitionID) HealthState {
	for _, p := range c.FetchMap().Partitions {
		if p.ID == id {
			return p.Health
		}
	}
	return Failed
}

func mapPrimary(mp *Map, id PartitionID) MachineID {
	for _, p := range mp.Partitions {
		if p.ID == id {
			return p.Primary
		}
	}
	return 0
}

func mapAddress(mp *Map, id PartitionID) string {
	for _, p := range mp.Partitions {
		if p.ID == id {
			return p.Address
		}
	}
	return ""
}

func addressOfMachine(machines []Machine, id MachineID) string {
	for _, mac := range machines {
		if mac.ID == id {
			return mac.Address
		}
	}
	return ""
}
