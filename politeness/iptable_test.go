package politeness

import (
	"testing"
	"time"
)

func ip(b byte) [16]byte {
	var a [16]byte
	a[15] = b
	return a
}

// TestIPTableGatesSharedAddress is the per-IP guarantee: two hosts on one
// address are throttled as a group, so the second cannot dispatch inside the
// first's interval even though each has its own host bucket.
func TestIPTableGatesSharedAddress(t *testing.T) {
	tab := NewIPTable(500 * time.Millisecond)
	addr := ip(7)

	if got := tab.EligibleAt(addr); got != 0 {
		t.Fatalf("fresh IP eligible at %d, want 0", got)
	}
	tab.Spend(addr, 1000, 2*time.Second)
	if got := tab.EligibleAt(addr); got != 1002 {
		t.Errorf("after spend eligible at %d, want 1002", got)
	}
}

// TestIPTableFloor checks an IP interval never drops below the per-IP floor even
// when a single host on it is allowed to go faster.
func TestIPTableFloor(t *testing.T) {
	tab := NewIPTable(3 * time.Second)
	addr := ip(9)
	tab.Spend(addr, 100, 1*time.Second) // host interval under the IP floor
	if got := tab.EligibleAt(addr); got != 103 {
		t.Errorf("floored eligible at %d, want 103 (3s floor)", got)
	}
}

// TestIPTableZeroIPNeverGates checks an unresolved host (the zero address) is
// never throttled by the IP table, so per-host politeness is the only rule until
// DNS lands an address.
func TestIPTableZeroIPNeverGates(t *testing.T) {
	tab := NewIPTable(time.Second)
	var zero [16]byte
	tab.Spend(zero, 1000, time.Hour)
	if got := tab.EligibleAt(zero); got != 0 {
		t.Errorf("zero IP eligible at %d, want 0 (never gated)", got)
	}
	if tab.Len() != 0 {
		t.Errorf("zero IP created a bucket, Len = %d", tab.Len())
	}
}

// TestIPTableEvict checks idle buckets are reclaimed and active ones kept, so
// the table stays bounded by the working set.
func TestIPTableEvict(t *testing.T) {
	tab := NewIPTable(time.Second)
	tab.Spend(ip(1), 100, time.Second)
	tab.Spend(ip(2), 500, time.Second)
	if tab.Len() != 2 {
		t.Fatalf("Len = %d, want 2", tab.Len())
	}
	n := tab.Evict(200) // ip(1) touched at 100 goes, ip(2) at 500 stays
	if n != 1 || tab.Len() != 1 {
		t.Errorf("Evict removed %d (Len %d), want 1 removed (Len 1)", n, tab.Len())
	}
}
