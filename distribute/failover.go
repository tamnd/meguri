package distribute

// Failover is the control plane's half of doc 12, section 5: it turns missed
// heartbeats into a health verdict and, on a confirmed machine loss, promotes the
// rendezvous-placed replica into the primary and bumps the epoch so the change
// orders against every other map change. Two failure modes get two answers. A
// crash-and-restart machine re-registers and resumes its own partitions from
// local recovery (Heartbeat restores it, no ownership moves). A lost machine
// never comes back, so its partitions move to the surviving replicas the
// preference list already named (FailMachine promotes them). The epoch is the
// split-brain guard: a recovered original learns the new epoch on its next
// heartbeat, sees its hosts reassigned, and steps down rather than double-crawl.

// Heartbeat records a live beat from a partition. It clears the partition's miss
// counter and, if the partition had been marked Degraded or Failed, restores it
// to Alive and bumps the epoch. This is the crash-and-restart path: a process
// that recovers locally and beats again reclaims its own partition with no
// ownership change.
func (c *Control) Heartbeat(id PartitionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.misses, id)
	for i := range c.m.Partitions {
		if c.m.Partitions[i].ID == id {
			if c.m.Partitions[i].Health != Alive {
				c.m.Partitions[i].Health = Alive
				c.bump()
			}
			return
		}
	}
}

// MissHeartbeat records one missed beat from a partition and returns the
// partition's resulting health. A first miss degrades the partition, riding out a
// transient stall; failThreshold consecutive misses fail it. It does not move
// ownership on its own: detection (the health verdict) and action (FailMachine)
// are kept separate so the control plane decides to fail over only on a confirmed
// machine loss, not on a slow heartbeat.
func (c *Control) MissHeartbeat(id PartitionID) HealthState {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.misses[id]++
	next := Degraded
	if c.misses[id] >= c.failThreshold {
		next = Failed
	}
	for i := range c.m.Partitions {
		if c.m.Partitions[i].ID == id {
			if c.m.Partitions[i].Health != next {
				c.m.Partitions[i].Health = next
				c.bump()
			}
			return next
		}
	}
	return next
}

// FailMachine removes a lost machine from the fleet and promotes every partition
// it held the primary for to that partition's highest-ranked surviving replica,
// which is exactly the next machine in the rendezvous preference list once the
// dead one is gone. It recomputes placement over the survivors in one pass, bumps
// the epoch once, and returns the partitions whose primary moved. Partitions that
// merely had the machine as a replica get a fresh replica from the same pass. A
// promotion costs nothing beyond loading the replica's recovered state, because
// the replica is already a partition caught up to the streamed tail (section 4).
func (c *Control) FailMachine(id MachineID) []PartitionID {
	c.mu.Lock()
	defer c.mu.Unlock()

	old := make(map[PartitionID]MachineID, len(c.m.Partitions))
	for _, p := range c.m.Partitions {
		old[p.ID] = p.Primary
	}

	kept := c.machines[:0:0]
	for _, mac := range c.machines {
		if mac.ID != id {
			kept = append(kept, mac)
		}
	}
	if len(kept) == len(c.machines) {
		return nil // not in the fleet, nothing to do
	}
	c.machines = kept

	c.replace()
	c.bump()

	var promoted []PartitionID
	for _, p := range c.m.Partitions {
		if p.Primary != old[p.ID] {
			promoted = append(promoted, p.ID)
		}
	}
	return promoted
}

// Primary returns the machine currently running a partition, and false if the
// partition is unknown.
func (c *Control) Primary(id PartitionID) (MachineID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.m.Partitions {
		if p.ID == id {
			return p.Primary, true
		}
	}
	return 0, false
}
