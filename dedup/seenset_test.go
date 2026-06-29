package dedup

import (
	"testing"

	"github.com/tamnd/meguri"
)

// TestSeenIdempotent is the headline guarantee: the first sight of a key is new
// (Seen false), every later sight is a rediscovery (Seen true), so a key
// delivered any number of times becomes one frontier entry (doc 08, section 9.3).
func TestSeenIdempotent(t *testing.T) {
	s := NewSeenSet()
	k := keyN(42)
	if s.Seen(k) {
		t.Fatal("first sight of a key reported as already seen")
	}
	for i := range 10 {
		if !s.Seen(k) {
			t.Fatalf("re-delivery %d reported as new", i)
		}
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after repeated delivery of one key", s.Len())
	}
}

// TestSeenZeroFalseNegatives is the correctness gate: over a stream with many
// duplicates, the seen-set must never call a genuinely-seen key new. A brute-force
// map is the oracle.
func TestSeenZeroFalseNegatives(t *testing.T) {
	s := NewSeenSet()
	oracle := map[meguri.URLKey]bool{}
	const stream = 300_000
	for i := range stream {
		// Draw from a key space a third the size, so two thirds are duplicates.
		k := keyN(i % (stream / 3))
		wasSeen := oracle[k]
		got := s.Seen(k)
		if wasSeen && !got {
			t.Fatalf("false negative at step %d: oracle says seen, set says new", i)
		}
		oracle[k] = true
	}
	if s.Len() != stream/3 {
		t.Fatalf("Len = %d, want %d distinct keys", s.Len(), stream/3)
	}
	t.Logf("%d distinct keys at %.2f bits/url", s.Len(), s.BitsPerURL())
}

// TestMergeMatchesSeen checks the batched DRUM path agrees with the single-key
// path: classifying a batch yields the same unique set as inserting one at a time.
func TestMergeMatchesSeen(t *testing.T) {
	keys := make([]meguri.URLKey, 0, 4000)
	for i := range 4000 {
		keys = append(keys, keyN(i%2500)) // 1500 duplicates
	}

	batched := NewSeenSet()
	out := batched.Merge(keys)
	uniqueBatched := 0
	for _, c := range out {
		if c.Unique {
			uniqueBatched++
		}
	}

	oneByOne := NewSeenSet()
	uniqueOne := 0
	for _, k := range keys {
		if !oneByOne.Seen(k) {
			uniqueOne++
		}
	}

	if uniqueBatched != uniqueOne {
		t.Fatalf("Merge unique count %d != Seen unique count %d", uniqueBatched, uniqueOne)
	}
	if uniqueBatched != 2500 {
		t.Fatalf("unique count = %d, want 2500", uniqueBatched)
	}
	if batched.Len() != oneByOne.Len() {
		t.Fatalf("Len mismatch: batched %d, one-by-one %d", batched.Len(), oneByOne.Len())
	}
}

// TestInsertRebuild checks the recovery path: Insert folds known-good keys in
// without classifying, and a later Seen of the same key reports a rediscovery, so
// a recovered partition dedups against everything it held.
func TestInsertRebuild(t *testing.T) {
	s := NewSeenSet()
	for i := range 1000 {
		s.Insert(keyN(i))
	}
	if s.Len() != 1000 {
		t.Fatalf("Len = %d after rebuild, want 1000", s.Len())
	}
	for i := range 1000 {
		if !s.Seen(keyN(i)) {
			t.Fatalf("rebuilt key %d not deduped", i)
		}
	}
}
