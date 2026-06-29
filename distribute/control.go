package distribute

import "sync"

// Control is the thin control plane: it owns the partition map and partition
// health and nothing else, and it is never on the data path (doc 12, section 2).
// Every change bumps the epoch, the one number that orders changes and guards
// against split-brain, and a partition learns of a change by comparing the epoch
// it sees on its heartbeat to its cached one, then pulling the new map. If the
// control plane is down the fleet keeps crawling against its last-known map; only
// the four control operations (add, remove, pin, fail over) wait on it.
type Control struct {
	mu            sync.Mutex
	m             *Map
	machines      []Machine           // the fleet, for rendezvous replica placement
	misses        map[PartitionID]int // consecutive missed heartbeats per partition
	backlog       map[PartitionID]int // latest pending depth each partition reported on its beat
	failThreshold int                 // missed heartbeats before a partition is Failed
}

// DefaultFailThreshold is the number of consecutive missed heartbeats that moves
// a partition from Degraded to Failed. A handful of misses rides out a transient
// stall; a sustained run is a real loss (doc 12, section 5).
const DefaultFailThreshold = 3

// NewControl starts a control plane with a single partition and no replicas,
// the smallest valid fleet, which a caller grows with AddPartition.
func NewControl() *Control {
	return &Control{
		m: &Map{
			Epoch:         1,
			NumPartitions: 1,
			Replicas:      0,
			Partitions:    []PartitionMeta{{ID: 0, Health: Alive}},
		},
		misses:        map[PartitionID]int{},
		backlog:       map[PartitionID]int{},
		failThreshold: DefaultFailThreshold,
	}
}

// FetchMap returns a snapshot of the current map for a router to cache. It is a
// clone, so the caller can swap it in without sharing the control plane's slices.
func (c *Control) FetchMap() *Map {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m.Clone()
}

// Epoch returns the current map epoch, the value a heartbeat compares against to
// decide whether to fetch.
func (c *Control) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m.Epoch
}

// NumPartitions returns the current partition count, the value the elasticity
// loop reads to decide whether it may still grow or shrink the fleet.
func (c *Control) NumPartitions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m.NumPartitions
}

// Backlog reports the latest per-partition pending depth the partitions sent on
// their heartbeats, indexed by PartitionID for the current partition count, so the
// control plane itself satisfies BacklogSource and the elasticity loop scales on
// the live fleet signal rather than a hand-fed slice (doc 12, section 7: the
// backlog rides the heartbeat the control plane already pulls). A partition that
// has not beat yet reads as zero, the safe default that never triggers a scale-up.
// A stale report for an id beyond the current count (a partition since removed) is
// dropped, so the slice length always matches NumPartitions, which is what the loop
// divides by.
func (c *Control) Backlog() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, c.m.NumPartitions)
	for id, n := range c.backlog {
		if int(id) < len(out) {
			out[id] = n
		}
	}
	return out
}

// SetReplicas sets the fleet-wide replication factor and recomputes every
// partition's replica set under the new N.
func (c *Control) SetReplicas(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m.Replicas = n
	c.replace()
}

// SetMachines records the fleet's machines and recomputes replica placement, the
// rendezvous mapping from partitions to the machines that hold their copies.
func (c *Control) SetMachines(machines []Machine) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.machines = append([]Machine(nil), machines...)
	c.replace()
}

// AddPartition appends a partition at the high end, the only growth jump hashing
// supports, bumps the count and epoch, and returns the new id. By minimal
// movement about 1/(n+1) of hosts now map to the new partition and none move
// between existing ones (doc 12, section 1).
func (c *Control) AddPartition(address string) PartitionID {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := PartitionID(c.m.NumPartitions)
	c.m.NumPartitions++
	c.m.Partitions = append(c.m.Partitions, PartitionMeta{ID: id, Address: address, Health: Alive})
	c.bump()
	c.replace()
	return id
}

// RemovePartition drops the highest-numbered partition, the only removal jump
// hashing supports cheaply; its hosts remap back across the survivors. It
// returns the removed id and true, or false if only one partition remains.
func (c *Control) RemovePartition() (PartitionID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m.NumPartitions <= 1 {
		return 0, false
	}
	id := PartitionID(c.m.NumPartitions - 1)
	c.m.NumPartitions--
	c.m.Partitions = c.m.Partitions[:len(c.m.Partitions)-1]
	delete(c.backlog, id) // drop the removed partition's stale backlog report
	c.bump()
	c.replace()
	return id, true
}

// Pin overrides the jump hash for one HostKey, routing it to a chosen partition
// regardless of the hash. It is how a middle drain or a hot-host isolation places
// specific hosts (doc 12, sections 1 and 7).
func (c *Control) Pin(hostKey uint64, to PartitionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m.Overrides == nil {
		c.m.Overrides = map[uint64]PartitionID{}
	}
	c.m.Overrides[hostKey] = to
	c.bump()
}

// SetHealth records a partition's health learned from heartbeats and bumps the
// epoch, so a failover that changes ownership is ordered like any other change.
func (c *Control) SetHealth(id PartitionID, h HealthState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.m.Partitions {
		if c.m.Partitions[i].ID == id {
			c.m.Partitions[i].Health = h
			c.bump()
			return
		}
	}
}

// bump increments the epoch; callers hold the lock.
func (c *Control) bump() { c.m.Epoch++ }

// replace recomputes every partition's primary and replica set from the current
// machine list by rendezvous placement; callers hold the lock. With no machines
// or no replica factor it clears the replica sets and leaves the primary as the
// single-box default. When a machine carries an Address the partition's routing
// address follows its primary, so a promotion that moves the primary also moves
// where discoveries land (doc 12, section 5).
func (c *Control) replace() {
	if len(c.machines) == 0 || c.m.Replicas <= 0 {
		for i := range c.m.Partitions {
			c.m.Partitions[i].Replicas = nil
		}
		return
	}
	weighted := false
	for _, mac := range c.machines {
		if mac.Weight != 0 && mac.Weight != 1 {
			weighted = true
			break
		}
	}
	for i := range c.m.Partitions {
		var pref []MachineID
		if weighted {
			pref = weightedPreference(c.m.Partitions[i].ID, c.machines, c.m.Replicas)
		} else {
			pref = preferenceList(c.m.Partitions[i].ID, machineIDs(c.machines), c.m.Replicas)
		}
		if len(pref) == 0 {
			c.m.Partitions[i].Replicas = nil
			continue
		}
		// The first entry is the primary; the rest are the replicas.
		c.m.Partitions[i].Primary = pref[0]
		if addr := c.addressOf(pref[0]); addr != "" {
			c.m.Partitions[i].Address = addr
		}
		if len(pref) > 1 {
			ids := make([]PartitionID, 0, len(pref)-1)
			for _, mid := range pref[1:] {
				ids = append(ids, PartitionID(mid))
			}
			c.m.Partitions[i].Replicas = ids
		} else {
			c.m.Partitions[i].Replicas = nil
		}
	}
}

// addressOf returns a machine's address, or "" if the machine is unknown or
// carries no address; callers hold the lock.
func (c *Control) addressOf(id MachineID) string {
	for _, mac := range c.machines {
		if mac.ID == id {
			return mac.Address
		}
	}
	return ""
}

func machineIDs(machines []Machine) []MachineID {
	out := make([]MachineID, len(machines))
	for i, mac := range machines {
		out[i] = mac.ID
	}
	return out
}
