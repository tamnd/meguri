package dedup

import (
	"bytes"
	"testing"

	"github.com/tamnd/meguri"
)

// TestFilterRoundTrip checks the marshal/unmarshal of the resident filter is
// exact: a reconstructed filter answers MaybeContains identically to the original
// for every key it holds and for a spread of keys it does not, and the blob is
// byte-stable so a checkpoint that did not touch the filter writes the same
// region.
func TestFilterRoundTrip(t *testing.T) {
	s := NewSeenSet(WithCapacity(4096))
	added := make([]meguri.URLKey, 0, 2000)
	for i := range 2000 {
		k := meguri.URLKey{HostKey: uint64(i % 37), PathKey: uint64(i) * 2654435761}
		s.Seen(k)
		added = append(added, k)
	}

	blob := s.MarshalFilter()
	if again := s.MarshalFilter(); !bytes.Equal(blob, again) {
		t.Fatal("MarshalFilter is not deterministic")
	}

	rf, err := UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(len(added)) {
		t.Fatalf("reconstructed filter holds %d keys, want %d", rf.Len(), len(added))
	}
	// Every added key must still probe true: the one-sided filter never loses a
	// key across the round trip.
	for i, k := range added {
		if !rf.MaybeContains(k) {
			t.Fatalf("added key %d went missing after round trip", i)
		}
	}
	// The reconstructed verdict must match the original bit for bit on unseen keys
	// too (same false positives, no new ones), the proof the bit array is identical.
	for i := range 5000 {
		k := meguri.URLKey{HostKey: uint64(i)*0x9E3779B97F4A7C15 + 1, PathKey: uint64(i)*11400714819323198485 + 7}
		if s.MaybeContains(k) != rf.MaybeContains(k) {
			t.Fatalf("verdict diverged on unseen key %d: original %v, reconstructed %v",
				i, s.MaybeContains(k), rf.MaybeContains(k))
		}
	}
}

// TestUnmarshalFilterRejectsGarbage checks the loader refuses a truncated or
// wrong-version blob rather than reading past the end.
func TestUnmarshalFilterRejectsGarbage(t *testing.T) {
	s := NewSeenSet(WithCapacity(64))
	s.Seen(meguri.URLKey{HostKey: 1, PathKey: 2})
	blob := s.MarshalFilter()

	for _, tc := range []struct {
		name string
		b    []byte
	}{
		{"empty", nil},
		{"truncated header", blob[:4]},
		{"truncated body", blob[:len(blob)-8]},
		{"bad version", append([]byte{9}, blob[1:]...)},
	} {
		if _, err := UnmarshalFilter(tc.b); err == nil {
			t.Fatalf("%s: expected an error, got nil", tc.name)
		}
	}
}

// TestCorpusFilterRoundTrip is the real-data gate for the seen-set filter region
// (doc 10 section 6): build the resident filter over every canonical key from the
// frozen CC-MAIN-2026-25 slice, serialize it, reconstruct it from the blob, and
// require the reconstructed filter to answer identically for every corpus key
// (zero false negatives, byte-identical verdicts) at the same resident cost. This
// proves the filter survives a checkpoint without a single re-add, the property
// the format region exists for.
func TestCorpusFilterRoundTrip(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(t, path)
	if len(keys) == 0 {
		t.Fatalf("corpus %s produced no canonical keys", path)
	}

	s := NewSeenSet(WithCapacity(uint64(len(keys))))
	for _, k := range keys {
		s.Seen(k)
	}

	blob := s.MarshalFilter()
	rf, err := UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(s.Len()) {
		t.Fatalf("reconstructed filter holds %d keys, set holds %d", rf.Len(), s.Len())
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("corpus key at position %d went missing after round trip", i)
		}
		if s.MaybeContains(k) != rf.MaybeContains(k) {
			t.Fatalf("verdict diverged on corpus key %d", i)
		}
	}
	t.Logf("corpus filter: %d distinct keys, %d blob bytes (%.2f bytes/url), %.2f resident bits/url",
		s.Len(), len(blob), float64(len(blob))/float64(s.Len()), rf.BitsPerURL())
}
