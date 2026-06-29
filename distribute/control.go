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
	mu       sync.Mutex
	m        *Map
	machines []Machine // the fleet, for rendezvous replica placement
}

// NewControl starts a control plane with a single partition and no replicas,
// the smallest valid fleet, which a caller grows with AddPartition.
func NewControl() *Control {
	return &Control{m: &Map{
		Epoch:         1,
		NumPartitions: 1,
		Replicas:      0,
		Partitions:    []PartitionMeta{{ID: 0, Health: Alive}},
	}}
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

// replace recomputes every partition's replica set from the current machine list
// by rendezvous placement; callers hold the lock. With no machines or no replica
// factor it clears the replica sets, the single-box default.
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
		// The first entry is the primary; the rest are the replicas.
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

func machineIDs(machines []Machine) []MachineID {
	out := make([]MachineID, len(machines))
	for i, mac := range machines {
		out[i] = mac.ID
	}
	return out
}
