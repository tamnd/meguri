package dedup

import (
	"testing"

	"github.com/tamnd/meguri"
)

// keyN derives a deterministic distinct URLKey for test index i, spreading both
// halves so the filter and the buckets see realistic distribution.
func keyN(i int) meguri.URLKey {
	return meguri.URLKey{
		HostKey: hashKey(meguri.URLKey{HostKey: uint64(i) * 0x100000001b3, PathKey: uint64(i)}),
		PathKey: hashKey(meguri.URLKey{HostKey: uint64(i), PathKey: uint64(i)*0x9E3779B1 + 7}),
	}
}

// TestFilterNoFalseNegatives is the property the whole seen-set rests on: a key
// that was added is never reported "not seen". A false negative would silently
// drop a real URL, so the filter must be strictly one-sided.
func TestFilterNoFalseNegatives(t *testing.T) {
	f := newFilter(100_000, 0.01)
	const n = 100_000
	for i := range n {
		f.add(keyN(i))
	}
	for i := range n {
		if !f.maybeSeen(keyN(i)) {
			t.Fatalf("false negative: key %d added but reported not seen", i)
		}
	}
}

// TestFilterFalsePositiveBudget checks that the realized false-positive rate on
// keys never added stays near the 1% budget, the throughput knob doc 08 sets.
func TestFilterFalsePositiveBudget(t *testing.T) {
	const n = 200_000
	f := newFilter(n, 0.01)
	for i := range n {
		f.add(keyN(i))
	}
	fp := 0
	const probes = 200_000
	for i := n; i < n+probes; i++ {
		if f.maybeSeen(keyN(i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	if rate > 0.03 {
		t.Errorf("false-positive rate %.4f exceeds the budget (target 1%%, ceiling 3%%)", rate)
	}
	t.Logf("false-positive rate %.4f at %.1f bits/key", rate, f.bitsPerKey())
}

// TestFilterBitsBudget checks the resident cost stays within the blocked-Bloom
// band the spec quotes (about 11 bits/url at 1%).
func TestFilterBitsBudget(t *testing.T) {
	const n = 500_000
	f := newFilter(n, 0.01)
	for i := range n {
		f.add(keyN(i))
	}
	if b := f.bitsPerKey(); b > 16 {
		t.Errorf("bits/url = %.2f, over the blocked-Bloom budget", b)
	}
}
