// Package distribute is the multi-partition layer (doc 12): the partition map
// that says which partition owns a HostKey, the router that sends a discovery to
// that owner, the control plane that owns the map and its epoch, and the
// rebalance that moves a host slice from one partition to another by shipping a
// .meguri file. A host lives entirely on one partition (D2), so every question
// the fleet has a shared answer to reduces to "which partition owns this
// HostKey", and the map answers it with jump consistent hashing and no directory.
package distribute

import "maps"

// PartitionID names a partition, the jump-hash bucket a HostKey maps to. It is
// the uint32 the .meguri header stamps as partition_id (doc 10).
type PartitionID uint32

// HealthState is what the control plane learns about a partition from its
// heartbeats. Draining is the pre-removal state that stops new hosts routing to
// a partition while its state ships off (doc 12, section 7).
type HealthState uint8

const (
	Alive HealthState = iota
	Degraded
	Failed
	Draining
)

// jumpHash maps a 64-bit key to a bucket in [0, numBuckets). It is the
// Lamping-Veach jump consistent hash: O(1) memory, no per-key state, and when
// numBuckets changes by one only ~1/numBuckets of keys move, all of them onto or
// off the highest bucket. numBuckets must be >= 1; a smaller count returns 0.
func jumpHash(key uint64, numBuckets int) int {
	if numBuckets <= 1 {
		return 0
	}
	var b int64 = -1
	var j int64 = 0
	for j < int64(numBuckets) {
		b = j
		// A linear-congruential step keeps the key evolving deterministically.
		key = key*2862933555777941757 + 1
		// The jump: (b+1) divided by the probability the key stays put gives the
		// next bucket the key decides to advance to.
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// Map is the entire partition map: a count, an epoch, a small override table,
// the replication factor, and a per-partition row. There is no HostKey
// directory; Owner recomputes ownership from the count every call, so the map a
// partition caches is a few kilobytes even for a fleet of thousands.
type Map struct {
	Epoch         uint64                 // bumped on every change, the change order
	NumPartitions int                    // the jump-hash bucket count
	Overrides     map[uint64]PartitionID // pinned HostKeys, usually empty
	Weights       []uint16               // per-partition weight for placement (section 7)
	Replicas      int                    // N, the replication factor (section 4)
	Partitions    []PartitionMeta        // id, address, HostKey range, health
}

// PartitionMeta is the per-partition row the control plane keeps for routing and
// health. The HostKey range mirrors what the partition's .meguri header records
// (doc 10), kept here so the manifest and the routers agree on the bounds.
type PartitionMeta struct {
	ID        PartitionID
	Address   string // where to send this partition's Discovery messages
	Health    HealthState
	HostKeyLo uint64
	HostKeyHi uint64
	Primary   MachineID     // the machine running this partition (0 when unplaced)
	Replicas  []PartitionID // the N-1 replicas for this partition's hosts
}

// Owner returns the partition that owns a HostKey under this map. It is the only
// place the partition count and the overrides are consulted, a pure function of
// the HostKey and the cached map: an override pin checked first, then the jump
// hash over the partition count.
func (m *Map) Owner(hostKey uint64) PartitionID {
	if pid, ok := m.Overrides[hostKey]; ok {
		return pid
	}
	return PartitionID(jumpHash(hostKey, m.NumPartitions))
}

// Clone returns a deep copy a router can swap in atomically without sharing the
// override or partition slices with the control plane that produced it.
func (m *Map) Clone() *Map {
	out := &Map{
		Epoch:         m.Epoch,
		NumPartitions: m.NumPartitions,
		Replicas:      m.Replicas,
	}
	if m.Overrides != nil {
		out.Overrides = make(map[uint64]PartitionID, len(m.Overrides))
		maps.Copy(out.Overrides, m.Overrides)
	}
	if m.Weights != nil {
		out.Weights = append([]uint16(nil), m.Weights...)
	}
	if m.Partitions != nil {
		out.Partitions = make([]PartitionMeta, len(m.Partitions))
		copy(out.Partitions, m.Partitions)
		for i := range out.Partitions {
			out.Partitions[i].Replicas = append([]PartitionID(nil), m.Partitions[i].Replicas...)
		}
	}
	return out
}
