package dedup

import (
	"math/bits"
	"testing"
)

// TestNearDupFindsWithinThreshold checks the permuted-table index returns a
// stored page whose simhash is within Hamming 3, the mirror-collapse query.
func TestNearDupFindsWithinThreshold(t *testing.T) {
	nd := NewNearDup()
	stored := uint64(0xA5A5A5A5A5A5A5A5)
	want := keyN(7)
	nd.Add(stored, want)

	// A query 3 bits away must be found.
	query := stored ^ 0b1011 // 3 bits flipped
	if bits.OnesCount64(stored^query) != 3 {
		t.Fatalf("test setup: distance is %d, want 3", bits.OnesCount64(stored^query))
	}
	got, ok := nd.Find(query)
	if !ok || got != want {
		t.Fatalf("Find within Hamming 3 = (%v, %v), want (%v, true)", got, ok, want)
	}
}

// TestNearDupMissesBeyondThreshold checks a query 4+ bits from everything stored
// is not a near-dup.
func TestNearDupMissesBeyondThreshold(t *testing.T) {
	nd := NewNearDup()
	stored := uint64(0xA5A5A5A5A5A5A5A5)
	nd.Add(stored, keyN(7))
	query := stored ^ 0b11110 // 4 bits flipped
	if _, ok := nd.Find(query); ok {
		t.Errorf("Find at distance 4 reported a near-dup")
	}
}

// TestNearDupExactRecall is the pigeonhole guarantee under load: over many stored
// simhashes, every query within Hamming 3 of a stored value is found, none is
// missed. A linear scan is the oracle.
func TestNearDupExactRecall(t *testing.T) {
	nd := NewNearDup()
	stored := make([]uint64, 0, 2000)
	for i := range 2000 {
		s := hashKey(keyN(i)) // well-spread 64-bit values
		stored = append(stored, s)
		nd.Add(s, keyN(i))
	}

	flips := []uint64{0, 0b1, 0b101, 0b10101} // distances 0,1,2,3
	for qi := range 2000 {
		for _, mask := range flips {
			query := stored[qi] ^ mask
			// Oracle: is any stored value within Hamming 3 of query?
			oracle := false
			for _, s := range stored {
				if bits.OnesCount64(s^query) <= 3 {
					oracle = true
					break
				}
			}
			_, ok := nd.Find(query)
			if ok != oracle {
				t.Fatalf("query %d mask %b: Find=%v oracle=%v", qi, mask, ok, oracle)
			}
		}
	}
}
