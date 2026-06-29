package dedup

import (
	"sort"
	"testing"

	"github.com/tamnd/meguri"
)

// TestExactSetAddContains checks the direct single-key path: a key is absent
// until added, present after, and a re-add is a no-op.
func TestExactSetAddContains(t *testing.T) {
	s := newExactSet(64)
	k := keyN(1)
	if s.contains(k) {
		t.Fatal("empty set reported a key present")
	}
	if !s.add(k) {
		t.Fatal("first add reported the key already present")
	}
	if !s.contains(k) {
		t.Fatal("added key reported absent")
	}
	if s.add(k) {
		t.Fatal("re-add reported the key as new")
	}
	if s.size != 1 {
		t.Fatalf("size = %d, want 1", s.size)
	}
}

// TestExactSetBucketsStaySorted checks the invariant the two-pointer merge relies
// on: every bucket is a sorted run after arbitrary inserts.
func TestExactSetBucketsStaySorted(t *testing.T) {
	s := newExactSet(16)
	for i := range 5000 {
		s.add(keyN(i))
	}
	for bi, b := range s.buckets {
		if !sort.SliceIsSorted(b, func(i, j int) bool { return b[i].Less(b[j]) }) {
			t.Fatalf("bucket %d not sorted after inserts", bi)
		}
	}
}

// TestDRUMMergeClassifies is the core DRUM gate: a batch merged against a bucket
// classifies each key as duplicate (already on disk) or unique (new, folded in),
// in one sequential pass.
func TestDRUMMergeClassifies(t *testing.T) {
	s := newExactSet(64)
	// Pre-load half the keys as the on-disk set.
	for i := range 1000 {
		s.add(keyN(i))
	}
	// Batch covers 0..1999: 0..999 are duplicates, 1000..1999 are unique.
	batch := make([]pendingKey, 0, 2000)
	for i := range 2000 {
		batch = append(batch, pendingKey{key: keyN(i), op: opCheckInsert})
	}
	out := s.merge(batch)
	if len(out) != 2000 {
		t.Fatalf("classified %d keys, want 2000", len(out))
	}
	uniq := map[meguri.URLKey]bool{}
	for _, c := range out {
		uniq[c.Key] = c.Unique
	}
	for i := range 2000 {
		wantUnique := i >= 1000
		if uniq[keyN(i)] != wantUnique {
			t.Fatalf("key %d: Unique = %v, want %v", i, uniq[keyN(i)], wantUnique)
		}
	}
	// After the merge every key is present.
	for i := range 2000 {
		if !s.contains(keyN(i)) {
			t.Fatalf("key %d absent after merge", i)
		}
	}
	if s.size != 2000 {
		t.Fatalf("size = %d, want 2000", s.size)
	}
}

// TestDRUMBatchInternalDuplicates checks that a key repeated within one batch is
// classified unique once and duplicate thereafter, so the same URL discovered
// twice in one window still becomes one entry.
func TestDRUMBatchInternalDuplicates(t *testing.T) {
	s := newExactSet(8)
	batch := []pendingKey{
		{key: keyN(5), op: opCheckInsert},
		{key: keyN(5), op: opCheckInsert},
		{key: keyN(5), op: opCheckInsert},
		{key: keyN(6), op: opCheckInsert},
	}
	out := s.merge(batch)
	uniqueCount := 0
	for _, c := range out {
		if c.Unique {
			uniqueCount++
		}
	}
	if uniqueCount != 2 {
		t.Fatalf("unique classifications = %d, want 2 (keyN(5) and keyN(6))", uniqueCount)
	}
	if s.size != 2 {
		t.Fatalf("size = %d, want 2", s.size)
	}
}

// TestBucketRouting checks a host group's keys land in one bucket, the colocation
// the DRUM merge needs (doc 08, section 9.2).
func TestBucketRouting(t *testing.T) {
	s := newExactSet(256)
	const hostKey = uint64(0xABCD000000000000)
	first := s.bucketOf(meguri.URLKey{HostKey: hostKey, PathKey: 0})
	for p := uint64(0); p < 1000; p++ {
		if s.bucketOf(meguri.URLKey{HostKey: hostKey, PathKey: p}) != first {
			t.Fatal("keys of one host group routed to different buckets")
		}
	}
}
