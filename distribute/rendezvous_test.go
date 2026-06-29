package distribute

import (
	"slices"
	"testing"
)

// TestPreferenceDeterministic checks every node computes the same ordered
// replica set for a partition from the same machine list, the no-allocator
// property placement needs.
func TestPreferenceDeterministic(t *testing.T) {
	a := preferenceList(7, []MachineID{10, 20, 30, 40, 50}, 2)
	b := preferenceList(7, []MachineID{50, 10, 40, 30, 20}, 2)
	if len(a) != 3 {
		t.Fatalf("want primary + 2 replicas, got %d", len(a))
	}
	if !slices.Equal(a, b) {
		t.Fatalf("preference depended on input order: %v vs %v", a, b)
	}
}

// TestPreferenceMinimalMovement checks that dropping a machine leaves the
// replica set unchanged for every partition that machine was not part of, the
// minimal reshuffle rendezvous guarantees.
func TestPreferenceMinimalMovement(t *testing.T) {
	full := []MachineID{1, 2, 3, 4, 5, 6, 7, 8}
	const dead MachineID = 4
	survivors := make([]MachineID, 0, len(full)-1)
	for _, mid := range full {
		if mid != dead {
			survivors = append(survivors, mid)
		}
	}
	for p := range PartitionID(200) {
		before := preferenceList(p, full, 2)
		if slices.Contains(before, dead) {
			continue // the dead machine was in this set, so a move is expected
		}
		after := preferenceList(p, survivors, 2)
		if !slices.Equal(before, after) {
			t.Fatalf("partition %d set changed though dead machine was not in it: %v -> %v", p, before, after)
		}
	}
}

// TestWeightedPreferenceBias checks a much heavier machine wins the primary slot
// for more partitions than an equal-weight peer would, the heterogeneity story.
func TestWeightedPreferenceBias(t *testing.T) {
	machines := []Machine{
		{ID: 1, Weight: 1}, {ID: 2, Weight: 1}, {ID: 3, Weight: 1}, {ID: 4, Weight: 8},
	}
	heavyWins, equalWins := 0, 0
	for p := range PartitionID(4000) {
		switch weightedPreference(p, machines, 1)[0] {
		case 4:
			heavyWins++
		case 1:
			equalWins++
		}
	}
	if heavyWins <= equalWins*2 {
		t.Fatalf("weight bias too weak: heavy won %d, an equal peer won %d", heavyWins, equalWins)
	}
}
