package distribute

import "sort"

// MachineID names a physical machine in the fleet. A machine hosts one or more
// partitions; a heavier machine hosts more, so heterogeneity lives in the
// placement layer, not the map (doc 12, section 7).
type MachineID uint32

// Machine is a fleet member and its capacity weight. Weight biases the
// rendezvous placement so a machine with twice the capacity wins the primary
// slot for about twice as many partitions. Address is where the machine accepts
// a partition's Discovery messages, carried so a promoted replica can publish the
// new owner's address into the map (doc 12, section 5).
type Machine struct {
	ID      MachineID
	Weight  float64
	Address string
}

// hashCombine mixes a partition id and a machine id into a rendezvous weight. It
// is a splitmix64 finalizer over the two values, so the weight is a
// well-distributed deterministic function every node computes identically.
func hashCombine(a, b uint64) uint64 {
	x := a*0x9E3779B97F4A7C15 ^ b
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

// preferenceList ranks machines for a partition by rendezvous (highest random
// weight) hashing, highest first. The first is the primary, the next
// replicaCount are the replicas. Every node computes the same list from the same
// machine set with no central allocator, and when a machine leaves only the
// partitions it was top-ranked for move, to the next-highest machine (doc 12,
// section 4).
func preferenceList(partition PartitionID, machines []MachineID, replicaCount int) []MachineID {
	type wm struct {
		w uint64
		m MachineID
	}
	ranked := make([]wm, len(machines))
	for i, mac := range machines {
		ranked[i] = wm{w: hashCombine(uint64(partition), uint64(mac)), m: mac}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].w != ranked[j].w {
			return ranked[i].w > ranked[j].w
		}
		return ranked[i].m < ranked[j].m // stable tiebreak on equal weight
	})
	n := min(replicaCount+1, len(ranked))
	out := make([]MachineID, 0, n)
	for i := range n {
		out = append(out, ranked[i].m)
	}
	return out
}

// weightedPreference is preferenceList biased by machine capacity: the
// rendezvous weight is scaled by the machine's Weight, the standard weighted-HRW
// form, so a heavier machine wins the primary slot for proportionally more
// partitions while the map itself stays equal-bucket jump hashing (doc 12,
// section 7).
func weightedPreference(partition PartitionID, machines []Machine, replicaCount int) []MachineID {
	type wm struct {
		w float64
		m MachineID
	}
	ranked := make([]wm, len(machines))
	for i, mac := range machines {
		raw := float64(hashCombine(uint64(partition), uint64(mac.ID)))
		ranked[i] = wm{w: raw * mac.Weight, m: mac.ID}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].w != ranked[j].w {
			return ranked[i].w > ranked[j].w
		}
		return ranked[i].m < ranked[j].m
	})
	n := min(replicaCount+1, len(ranked))
	out := make([]MachineID, 0, n)
	for i := range n {
		out = append(out, ranked[i].m)
	}
	return out
}
