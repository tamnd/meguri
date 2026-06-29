package distribute

import (
	"math"
	"testing"
)

// TestJumpHashRange checks the hash stays in bounds for a spread of counts and
// keys, the basic contract Owner relies on.
func TestJumpHashRange(t *testing.T) {
	for _, n := range []int{1, 2, 7, 100, 3334} {
		for k := range 5000 {
			b := jumpHash(uint64(k)*2654435761, n)
			if b < 0 || b >= n {
				t.Fatalf("jumpHash(%d,%d)=%d out of [0,%d)", k, n, b, n)
			}
		}
	}
}

// TestJumpHashBalance checks the hash spreads keys near-uniformly, so no
// partition is hot or cold from the hash itself. With 16 buckets over 160k keys
// the expected share is 10k each; allow a generous 15 percent band.
func TestJumpHashBalance(t *testing.T) {
	const n, keys = 16, 160000
	var count [n]int
	for k := range keys {
		count[jumpHash(splitmix(uint64(k)), n)]++
	}
	mean := float64(keys) / n
	for b, c := range count {
		dev := math.Abs(float64(c)-mean) / mean
		if dev > 0.15 {
			t.Fatalf("bucket %d got %d, mean %.0f, deviation %.1f%% over 15%%", b, c, mean, dev*100)
		}
	}
}

// TestJumpHashMinimalMovement is the property the whole rebalance economy rests
// on: going from n to n+1 partitions moves about 1/(n+1) of keys, and every key
// that moves goes onto the new highest bucket, none between existing buckets.
func TestJumpHashMinimalMovement(t *testing.T) {
	const n, keys = 1000, 200000
	moved, misdirected := 0, 0
	for k := range keys {
		key := splitmix(uint64(k))
		before := jumpHash(key, n)
		after := jumpHash(key, n+1)
		if before != after {
			moved++
			if after != n { // the only legal destination is the new bucket
				misdirected++
			}
		}
	}
	if misdirected != 0 {
		t.Fatalf("%d keys moved between existing buckets, expected 0", misdirected)
	}
	frac := float64(moved) / keys
	want := 1.0 / float64(n+1)
	// The moved fraction should sit near 1/(n+1); allow a wide band for sampling.
	if frac < want*0.5 || frac > want*2.0 {
		t.Fatalf("moved fraction %.4f, expected near %.4f", frac, want)
	}
}

// TestOwnerOverride checks a pin wins over the hash, the placement hook the
// elasticity operations use.
func TestOwnerOverride(t *testing.T) {
	m := &Map{NumPartitions: 8, Overrides: map[uint64]PartitionID{42: 3}}
	if got := m.Owner(42); got != 3 {
		t.Fatalf("override ignored: Owner(42)=%d want 3", got)
	}
	// A non-pinned key falls through to the hash and stays in range.
	if got := m.Owner(43); got >= 8 {
		t.Fatalf("Owner(43)=%d out of range", got)
	}
}

// TestMapCloneIndependent checks a cloned map shares no mutable state with its
// source, so a router can swap one in without aliasing the control plane.
func TestMapCloneIndependent(t *testing.T) {
	src := &Map{
		Epoch:         5,
		NumPartitions: 4,
		Overrides:     map[uint64]PartitionID{1: 2},
		Weights:       []uint16{1, 1, 1, 1},
		Partitions:    []PartitionMeta{{ID: 0, Replicas: []PartitionID{1}}},
	}
	cp := src.Clone()
	cp.Overrides[1] = 9
	cp.Weights[0] = 7
	cp.Partitions[0].Replicas[0] = 9
	if src.Overrides[1] != 2 || src.Weights[0] != 1 || src.Partitions[0].Replicas[0] != 1 {
		t.Fatal("clone aliased the source's slices or map")
	}
}

// splitmix scrambles a counter into a well-distributed 64-bit key so the balance
// and movement tests do not feed the hash a low-entropy sequence.
func splitmix(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}
