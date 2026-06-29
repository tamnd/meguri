package distribute

import (
	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// MovingHosts returns the HostKeys this partition holds under the old map but no
// longer owns under the new one, by scanning the small host table once and
// testing each key against the new map (doc 12, section 3). These are the hosts
// whose rows the rebalance slices out and ships; the large URL table is never
// scanned to find them.
func MovingHosts(hosts []m.HostRecord, self PartitionID, nm *Map) []uint64 {
	var moving []uint64
	for i := range hosts {
		if nm.Owner(hosts[i].HostKey) != self {
			moving = append(moving, hosts[i].HostKey)
		}
	}
	return moving
}

// Redistribute computes a rebalance for a source partition whose ownership map
// changed: it groups the hosts that moved by their new owner, slices each
// destination's hosts out as its own .meguri-ready partition stamped with the
// destination id, and returns those ship slices plus the keep partition the
// source retains. A host moves whole or not at all, because politeness is
// per-host (D2), so the slices and the keep partition never split a host.
//
// The move is a file operation: each slice is a normal partition the source
// encodes and ships, and the destination merges it into its live store (doc 11).
// Because the URL table is sorted by HostKey, slicing a moving host is a
// contiguous range, so the redistribution cost is the moved bytes over the
// bandwidth wall, not a row-by-row migration (doc 12, sections 3 and 8).
func Redistribute(src *format.Partition, self PartitionID, nm *Map) (ship map[PartitionID]*format.Partition, keep *format.Partition) {
	// Group the moving hosts by their new owner with a single host-table scan.
	byDest := map[PartitionID]map[uint64]bool{}
	for i := range src.Hosts {
		hk := src.Hosts[i].HostKey
		owner := nm.Owner(hk)
		if owner == self {
			continue
		}
		set := byDest[owner]
		if set == nil {
			set = map[uint64]bool{}
			byDest[owner] = set
		}
		set[hk] = true
	}

	ship = map[PartitionID]*format.Partition{}
	keep = src
	for dest, set := range byDest {
		out, rest := format.Extract(keep, set)
		out.ID = uint32(dest)
		ship[dest] = out
		keep = rest
	}
	keep.ID = uint32(self)
	return ship, keep
}
