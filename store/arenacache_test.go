package store

import (
	"fmt"
	"testing"
)

// TestArenaCacheLRUOrder checks the cache evicts least-recently-used first: with
// room for two entries, touching A then inserting B then C evicts A (the LRU),
// keeps B and C. This is the host-burst locality doc 05 section 3d depends on, so
// the freshly-dispatched strings stay hot and only cold ones miss.
func TestArenaCacheLRUOrder(t *testing.T) {
	// Each entry costs len(s)+48; pick strings so exactly two fit.
	const s = "0123456789" // 10 bytes -> cost 58
	budget := int64(2 * (len(s) + arenaEntryOverhead))
	c := newArenaCache(budget)

	c.put(1, s)
	c.put(2, s)
	if _, ok := c.get(1); !ok { // touch 1, now MRU; 2 is LRU
		t.Fatal("offset 1 should be present")
	}
	c.put(3, s) // evicts the LRU, which is offset 2
	if _, ok := c.get(2); ok {
		t.Fatal("offset 2 should have been evicted as LRU")
	}
	if _, ok := c.get(1); !ok {
		t.Fatal("offset 1 was touched and must survive")
	}
	if _, ok := c.get(3); !ok {
		t.Fatal("offset 3 was just inserted and must be present")
	}
	if used, b, _, _, _ := c.stats(); used > b {
		t.Fatalf("resident %d over budget %d", used, b)
	}
}

// TestArenaCacheByteBudget feeds many entries and asserts the resident bytes
// never exceed the budget and at least one eviction happened, the flat-B_arena
// residency the spill sells (doc 03).
func TestArenaCacheByteBudget(t *testing.T) {
	budget := int64(4 << 10)
	c := newArenaCache(budget)
	for i := range 1000 {
		c.put(uint64(i+1), fmt.Sprintf("https://h%d.example.com/p/%d", i, i))
		if used, b, _, _, _ := c.stats(); used > b {
			t.Fatalf("put %d: resident %d over budget %d", i, used, b)
		}
	}
	if _, _, _, _, evicted := c.stats(); evicted == 0 {
		t.Fatal("expected evictions filling a small budget with 1000 entries")
	}
}

// TestArenaCacheZeroBudget checks the explicit no-cache floor: a zero budget
// never holds bytes and always misses, so the spilled arena degrades to a pure
// pread path (doc 05 section 3).
func TestArenaCacheZeroBudget(t *testing.T) {
	c := newArenaCache(0)
	c.put(1, "https://example.com/")
	if _, ok := c.get(1); ok {
		t.Fatal("zero-budget cache must never hold an entry")
	}
	if used, _, _, _, _ := c.stats(); used != 0 {
		t.Fatalf("zero-budget resident bytes = %d, want 0", used)
	}
}

// TestArenaCacheDrop checks drop clears all residency, the checkpoint re-warm
// path (doc 05 section 7b): after a checkpoint rewrites the snapshot the cache is
// dropped and re-warms lazily.
func TestArenaCacheDrop(t *testing.T) {
	c := newArenaCache(1 << 20)
	for i := range 50 {
		c.put(uint64(i+1), "https://example.com/page")
	}
	c.drop()
	if used, _, _, _, _ := c.stats(); used != 0 {
		t.Fatalf("after drop resident bytes = %d, want 0", used)
	}
	if _, ok := c.get(1); ok {
		t.Fatal("after drop no offset should be present")
	}
}
