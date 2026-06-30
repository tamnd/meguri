package distribute

import (
	"hash/crc32"
	"slices"

	"github.com/tamnd/meguri/format"
)

// This file is the safe-move handshake (doc 12, sections 3 and 4): the protocol
// that carries a moving host from a source partition into its new owner's live
// store without ever losing it. Redistribute computes which hosts move and slices
// them out; Handoff is the durability discipline on top of that slice. The rule is
// ship-then-delete, never delete-then-ship: the source keeps serving a host until
// the destination acknowledges it durably holds the slice, so at every instant a
// host is owned by the source, the destination, or both, never by neither. The
// move is at-least-once and idempotent (D16): a redelivered slice merges to the
// same state, and a lost ack only keeps a host on the source for one more round.

// crc32cTable is the Castagnoli polynomial the file format checksums with, reused
// here so the slice integrity check on the wire matches the on-disk one.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// SliceChecksum is the CRC32C over an encoded partition slice, the integrity tag
// the source computes before shipping and the destination echoes in its ack. A
// mismatch means the bytes that arrived are not the bytes that left, so the source
// must not delete its copy.
func SliceChecksum(slice []byte) uint32 {
	return crc32.Checksum(slice, crc32cTable)
}

// MoveAck is the destination's acknowledgement of a shipped slice. The source
// deletes the moved hosts from its own partition only when the ack confirms the
// destination committed the slice durably (Committed), at the map epoch the move
// was computed for (Epoch), with the bytes intact (Checksum matches what the
// source shipped). Any mismatch leaves the hosts on the source.
type MoveAck struct {
	Dest      PartitionID
	Epoch     uint64
	Checksum  uint32
	Committed bool
}

// MoveTransport ships an encoded partition slice to its new owner and returns the
// owner's acknowledgement. The in-process implementation merges the slice into a
// local destination store and acks; the cross-machine implementation sends the
// bytes over the wire and waits for the remote ack. The source treats both the
// same way, the way the discovery and replication seams have one local and one
// remote implementation (doc 04, the transport binding). A transport error is not
// fatal to the rebalance: the source keeps that destination's hosts and the move
// retries next round.
type MoveTransport interface {
	Ship(dest PartitionID, slice []byte, epoch uint64, checksum uint32) (MoveAck, error)
}

// Handoff ships every moving host slice to its new owner under the new map and
// drops from the source only the hosts whose destination acknowledged a durable,
// epoch-matching, checksum-matching commit. It returns the kept partition the
// source retains (still owning every host whose move did not complete) and the
// sorted list of destinations that took their slice.
//
// The contract is the safe-move invariant: a host leaves the source only after its
// new owner durably holds it, so a crash or a dropped ack at any point loses no
// host. A destination that errors, fails to commit, or returns a mismatched epoch
// or checksum keeps its hosts on the source, where they are still served and still
// shipped next round. An encode failure on a slice is the one fatal error, since it
// means the source could not produce the bytes to ship at all.
func Handoff(src *format.Partition, self PartitionID, nm *Map, epoch uint64, t MoveTransport) (kept *format.Partition, moved []PartitionID, err error) {
	// Group the moving hosts by their new owner with a single host-table scan, the
	// same grouping Redistribute does; doing it here lets Handoff commit each
	// destination's drop independently on its ack.
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

	// Ship to destinations in id order so a run is deterministic regardless of map
	// iteration order.
	dests := make([]PartitionID, 0, len(byDest))
	for dest := range byDest {
		dests = append(dests, dest)
	}
	slices.Sort(dests)

	keep := src
	for _, dest := range dests {
		set := byDest[dest]
		// Slice the destination's hosts off a copy: out is the slice to ship, rest
		// is what the source would retain if the move commits. Extract does not
		// mutate keep, so an unacknowledged move simply discards out and rest.
		out, rest := format.Extract(keep, set)
		out.ID = uint32(dest)
		blob, encErr := format.Encode(out)
		if encErr != nil {
			return nil, nil, encErr
		}
		sum := SliceChecksum(blob)

		ack, shipErr := t.Ship(dest, blob, epoch, sum)
		if shipErr != nil {
			continue // transport failed: keep the hosts, retry next round
		}
		if !ack.Committed || ack.Dest != dest || ack.Epoch != epoch || ack.Checksum != sum {
			continue // not durably committed as shipped: keep the hosts
		}
		// The destination durably holds the slice: now it is safe to drop it here.
		keep = rest
		moved = append(moved, dest)
	}
	keep.ID = uint32(self)
	return keep, moved, nil
}
