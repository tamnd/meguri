package distribute

import (
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// fleetTransport is the in-process MoveTransport double: it stands in for the
// destinations' live stores so the ship-then-delete handshake runs end to end with
// no network. A destination commits a slice by folding its rows into a key-keyed
// set (last-writer-wins, the log-structured store's idempotent merge, doc 11), so a
// redelivered slice lands the same state. failCommit models a destination that
// could not durably commit, and corrupt models a damaged ack: both must leave the
// shipped hosts on the source.
type fleetTransport struct {
	urls       map[PartitionID]map[m.URLKey]bool
	hosts      map[PartitionID]map[uint64]bool
	failCommit map[PartitionID]bool
	corrupt    map[PartitionID]bool
	shipped    map[PartitionID]int // how many times each dest was shipped to
}

func newFleet() *fleetTransport {
	return &fleetTransport{
		urls:       map[PartitionID]map[m.URLKey]bool{},
		hosts:      map[PartitionID]map[uint64]bool{},
		failCommit: map[PartitionID]bool{},
		corrupt:    map[PartitionID]bool{},
		shipped:    map[PartitionID]int{},
	}
}

func (f *fleetTransport) Ship(dest PartitionID, slice []byte, epoch uint64, checksum uint32) (MoveAck, error) {
	f.shipped[dest]++
	got := SliceChecksum(slice)
	// A checksum mismatch or a destination that cannot commit acks uncommitted, so
	// the source keeps the hosts.
	if got != checksum || f.failCommit[dest] {
		return MoveAck{Dest: dest, Epoch: epoch, Checksum: got, Committed: false}, nil
	}
	part, err := format.Decode(slice)
	if err != nil {
		return MoveAck{}, err
	}
	if f.urls[dest] == nil {
		f.urls[dest] = map[m.URLKey]bool{}
		f.hosts[dest] = map[uint64]bool{}
	}
	for _, u := range part.URLs {
		f.urls[dest][u.URLKey] = true
	}
	for _, h := range part.Hosts {
		f.hosts[dest][h.HostKey] = true
	}
	ack := MoveAck{Dest: dest, Epoch: epoch, Checksum: got, Committed: true}
	if f.corrupt[dest] {
		ack.Checksum = got ^ 1 // a damaged ack: committed, but the source cannot trust it
	}
	return ack, nil
}

func hostSet(p *format.Partition) map[uint64]bool {
	s := map[uint64]bool{}
	for _, h := range p.Hosts {
		s[h.HostKey] = true
	}
	return s
}

// TestHandoffShipsThenDeletes is the safe-move gate: only the hosts whose
// destination durably commits with a matching epoch and checksum leave the source.
// A destination that cannot commit (failCommit) and one whose ack is damaged
// (corrupt) both keep their hosts on the source, so no host is ever owned by
// neither side. Hosts 20,21 move to a healthy dest 1; 30,31 to a failing dest 2;
// 40,41 to a corrupt-ack dest 3; 10,11 stay on self.
func TestHandoffShipsThenDeletes(t *testing.T) {
	src := buildPartition([]uint64{10, 11, 20, 21, 30, 31, 40, 41})
	nm := &Map{
		Epoch:         7,
		NumPartitions: 4,
		Overrides: map[uint64]PartitionID{
			10: 0, 11: 0,
			20: 1, 21: 1,
			30: 2, 31: 2,
			40: 3, 41: 3,
		},
	}
	fleet := newFleet()
	fleet.failCommit[2] = true
	fleet.corrupt[3] = true

	kept, moved, err := Handoff(src, 0, nm, nm.Epoch, fleet)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}

	if len(moved) != 1 || moved[0] != 1 {
		t.Fatalf("moved = %v, want only dest 1 committed", moved)
	}

	keptHosts := hostSet(kept)
	// Self's hosts plus the two failed destinations' hosts stay on the source.
	for _, hk := range []uint64{10, 11, 30, 31, 40, 41} {
		if !keptHosts[hk] {
			t.Fatalf("host %d was dropped from the source but its move did not commit", hk)
		}
	}
	// Only the healthy destination's hosts left the source.
	for _, hk := range []uint64{20, 21} {
		if keptHosts[hk] {
			t.Fatalf("host %d still on the source after a committed move", hk)
		}
	}
	if kept.ID != 0 {
		t.Fatalf("kept partition id = %d, want self 0", kept.ID)
	}

	// The healthy destination durably holds exactly its hosts.
	for _, hk := range []uint64{20, 21} {
		if !fleet.hosts[1][hk] {
			t.Fatalf("dest 1 did not durably hold host %d", hk)
		}
	}
	// The failed destination committed nothing.
	if len(fleet.hosts[2]) != 0 {
		t.Fatalf("dest 2 holds %d hosts but it could not commit", len(fleet.hosts[2]))
	}

	// The safe-move invariant: every source host is owned by the source, a
	// destination that committed it, or both, never by neither.
	for _, hk := range []uint64{10, 11, 20, 21, 30, 31, 40, 41} {
		ownedSomewhere := keptHosts[hk] || fleet.hosts[1][hk] || fleet.hosts[3][hk]
		if !ownedSomewhere {
			t.Fatalf("host %d is owned by no one after the handoff", hk)
		}
	}
}

// TestHandoffAllHealthy checks the clean path: when every destination commits, the
// source keeps only the hosts it still owns and each destination holds its slice.
func TestHandoffAllHealthy(t *testing.T) {
	src := buildPartition([]uint64{10, 11, 20, 21, 30})
	nm := &Map{
		Epoch:         3,
		NumPartitions: 3,
		Overrides:     map[uint64]PartitionID{10: 0, 11: 0, 20: 1, 21: 1, 30: 2},
	}
	fleet := newFleet()

	kept, moved, err := Handoff(src, 0, nm, nm.Epoch, fleet)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if len(moved) != 2 || moved[0] != 1 || moved[1] != 2 {
		t.Fatalf("moved = %v, want [1 2] in id order", moved)
	}
	keptHosts := hostSet(kept)
	if len(keptHosts) != 2 || !keptHosts[10] || !keptHosts[11] {
		t.Fatalf("kept hosts = %v, want only self's 10,11", keptHosts)
	}
	if !fleet.hosts[1][20] || !fleet.hosts[1][21] || !fleet.hosts[2][30] {
		t.Fatal("a destination did not durably hold its slice")
	}
}

// TestHandoffIdempotentRedelivery checks the move is safe to retry: running the
// handoff again from the kept partition ships nothing to a destination that already
// took its hosts, and re-ships only the ones still pending. A redelivery to a
// destination that committed before lands the same state.
func TestHandoffIdempotentRedelivery(t *testing.T) {
	src := buildPartition([]uint64{10, 20, 21, 30})
	nm := &Map{
		Epoch:         5,
		NumPartitions: 3,
		Overrides:     map[uint64]PartitionID{10: 0, 20: 1, 21: 1, 30: 2},
	}
	fleet := newFleet()
	fleet.failCommit[2] = true // dest 2 fails the first round

	kept1, moved1, err := Handoff(src, 0, nm, nm.Epoch, fleet)
	if err != nil {
		t.Fatalf("first handoff: %v", err)
	}
	if len(moved1) != 1 || moved1[0] != 1 {
		t.Fatalf("first round moved = %v, want [1]", moved1)
	}

	// Dest 2 recovers; retry from the kept partition.
	fleet.failCommit[2] = false
	kept2, moved2, err := Handoff(kept1, 0, nm, nm.Epoch, fleet)
	if err != nil {
		t.Fatalf("second handoff: %v", err)
	}
	if len(moved2) != 1 || moved2[0] != 2 {
		t.Fatalf("second round moved = %v, want [2] (dest 1 already done)", moved2)
	}
	// Dest 1 was shipped to once, not again: its hosts left the source after round 1.
	if fleet.shipped[1] != 1 {
		t.Fatalf("dest 1 shipped %d times, want 1 (no needless redelivery)", fleet.shipped[1])
	}
	keptHosts := hostSet(kept2)
	if len(keptHosts) != 1 || !keptHosts[10] {
		t.Fatalf("after both rounds kept = %v, want only self's host 10", keptHosts)
	}
	if !fleet.hosts[1][20] || !fleet.hosts[1][21] || !fleet.hosts[2][30] {
		t.Fatal("a destination is missing its hosts after the retry")
	}
}
