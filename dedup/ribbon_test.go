package dedup

import (
	"testing"

	"github.com/tamnd/meguri"
)

// makeKeys builds n distinct URLKeys from a seeded sequence, a stand-in for a
// canonicalized key set when no corpus is loaded.
func makeKeys(n int) []meguri.URLKey {
	keys := make([]meguri.URLKey, n)
	for i := range keys {
		h := ribbonMix(uint64(i) * 0x9E3779B97F4A7C15)
		p := ribbonMix(uint64(i)*0xC2B2AE3D27D4EB4F + 1)
		keys[i] = meguri.URLKey{HostKey: h, PathKey: p}
	}
	return keys
}

// absentKeys builds n distinct keys disjoint from makeKeys, for measuring the
// false-positive rate.
func absentKeys(n int) []meguri.URLKey {
	keys := make([]meguri.URLKey, n)
	for i := range keys {
		h := ribbonMix(uint64(i)*0x9E3779B97F4A7C15 + 0xDEADBEEF)
		p := ribbonMix(uint64(i)*0xC2B2AE3D27D4EB4F + 0xFEEDFACE)
		keys[i] = meguri.URLKey{HostKey: h, PathKey: p}
	}
	return keys
}

// TestRibbonNoFalseNegative is the one-sided guarantee: every key built into the
// ribbon must probe true, no matter the count, because a false negative would
// drop a genuinely seen url.
func TestRibbonNoFalseNegative(t *testing.T) {
	for _, n := range []int{0, 1, 2, 63, 64, 65, 1000, 50000} {
		keys := makeKeys(n)
		rb, err := buildRibbon(keys, defaultRibbonR)
		if err != nil {
			t.Fatalf("n=%d build: %v", n, err)
		}
		for i, k := range keys {
			if !rb.query(k) {
				t.Fatalf("n=%d: key %d false-negative, the one-sided contract broke", n, i)
			}
		}
	}
}

// TestRibbonRoundTrip checks the serialized ribbon reconstructs a filter that
// answers identically: every member still probes true and the false-positive rate
// stays near the 2^-r the fingerprint width sets.
func TestRibbonRoundTrip(t *testing.T) {
	keys := makeKeys(20000)
	blob, err := BuildRibbonFilter(keys)
	if err != nil {
		t.Fatalf("BuildRibbonFilter: %v", err)
	}
	if blob[1] != filterKindRibbon {
		t.Fatalf("blob kind = %d, want ribbon (%d)", blob[1], filterKindRibbon)
	}
	rf, err := UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(len(keys)) {
		t.Fatalf("reconstructed filter holds %d keys, want %d", rf.Len(), len(keys))
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("member %d went missing after round trip", i)
		}
	}

	// False-positive rate: absent keys match with about 2^-r probability.
	absent := absentKeys(100000)
	fp := 0
	for _, k := range absent {
		if rf.MaybeContains(k) {
			fp++
		}
	}
	rate := float64(fp) / float64(len(absent))
	want := 1.0 / float64(int(1)<<defaultRibbonR) // 2^-7 = 0.78%
	if rate > want*2 {
		t.Fatalf("false-positive rate %.4f exceeds 2x the target %.4f", rate, want)
	}
	t.Logf("ribbon: %d keys, %d blob bytes (%.2f bytes/url, %.2f bits/url), fp rate %.4f (target %.4f)",
		len(keys), len(blob), float64(len(blob))/float64(len(keys)), rf.BitsPerURL(), rate, want)
}

// TestRibbonBitsKnob checks a wider fingerprint trades bits-per-url for a lower
// false-positive rate, and that the membership guarantee holds at each width.
func TestRibbonBitsKnob(t *testing.T) {
	keys := makeKeys(10000)
	var prev float64
	for _, r := range []int{6, 8, 10} {
		blob, err := BuildRibbonFilter(keys, WithRibbonBits(r))
		if err != nil {
			t.Fatalf("r=%d build: %v", r, err)
		}
		rf, err := UnmarshalFilter(blob)
		if err != nil {
			t.Fatalf("r=%d unmarshal: %v", r, err)
		}
		for i, k := range keys {
			if !rf.MaybeContains(k) {
				t.Fatalf("r=%d: member %d false-negative", r, i)
			}
		}
		bpu := rf.BitsPerURL()
		if bpu <= prev {
			t.Fatalf("r=%d bits/url %.2f did not grow past the previous %.2f", r, bpu, prev)
		}
		prev = bpu
	}
}

// TestRibbonRejectsGarbage checks the reader refuses a truncated or malformed
// ribbon blob rather than panicking or fabricating a filter.
func TestRibbonRejectsGarbage(t *testing.T) {
	good, err := BuildRibbonFilter(makeKeys(500))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	cases := map[string][]byte{
		"empty":            {},
		"header only":      good[:ribbonHeaderSize],
		"truncated body":   good[:len(good)-1],
		"bad version":      append([]byte{0xFF}, good[1:]...),
		"zero fingerprint": withByte(good, 2, 0),
		"oversize r":       withByte(good, 2, maxRibbonRBits+1),
	}
	for name, blob := range cases {
		if _, err := UnmarshalFilter(blob); err == nil {
			t.Fatalf("%s: UnmarshalFilter accepted a malformed ribbon blob", name)
		}
	}
}

func withByte(b []byte, i int, v byte) []byte {
	out := append([]byte(nil), b...)
	out[i] = v
	return out
}

// TestSeenSetMarshalRibbon checks the live seen-set freezes into a ribbon that
// holds exactly the keys the set holds.
func TestSeenSetMarshalRibbon(t *testing.T) {
	keys := makeKeys(8000)
	s := NewSeenSet(WithCapacity(uint64(len(keys))))
	for _, k := range keys {
		s.Seen(k)
	}
	blob, err := s.MarshalRibbon()
	if err != nil {
		t.Fatalf("MarshalRibbon: %v", err)
	}
	rf, err := UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(s.Len()) {
		t.Fatalf("ribbon holds %d keys, set holds %d", rf.Len(), s.Len())
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("set key %d missing from the ribbon snapshot", i)
		}
	}
}

// TestCorpusRibbonFilter is the real-data gate for the cold ribbon form (doc 08,
// section 3.2): over the frozen ccrawl slice the ribbon must hold every distinct
// key with zero false negatives through an on-disk round trip, cost about 7
// bits/url (a clear win over the blocked-Bloom 11.29), and keep its false-positive
// rate near 2^-r. It reports the numbers the budget is judged on.
func TestCorpusRibbonFilter(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(t, path)
	if len(keys) == 0 {
		t.Fatalf("corpus %s produced no canonical keys", path)
	}

	blob, err := BuildRibbonFilter(keys)
	if err != nil {
		t.Fatalf("BuildRibbonFilter: %v", err)
	}
	rf, err := UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("UnmarshalFilter: %v", err)
	}
	if rf.Len() != uint64(len(keys)) {
		t.Fatalf("ribbon holds %d keys, corpus has %d", rf.Len(), len(keys))
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("corpus key %d went missing after the on-disk round trip", i)
		}
	}
	if bpu := rf.BitsPerURL(); bpu > 9.0 {
		t.Fatalf("ribbon cost %.2f bits/url, want a clear win under the blocked-Bloom 11.29", bpu)
	}

	absent := absentKeys(len(keys))
	fp := 0
	for _, k := range absent {
		if rf.MaybeContains(k) {
			fp++
		}
	}
	rate := float64(fp) / float64(len(absent))
	t.Logf("corpus ribbon: %d distinct keys, %d blob bytes (%.2f bytes/url, %.2f bits/url), fp rate %.4f vs target %.4f",
		len(keys), len(blob), float64(len(blob))/float64(len(keys)), rf.BitsPerURL(),
		rate, 1.0/float64(int(1)<<defaultRibbonR))
}
