package dedup

import (
	"math/bits"
	"strings"
	"testing"
)

// pageText is document-length prose, the scale simhash near-dup is designed for:
// a 64-bit signature separates near-dups from distinct pages when a page has many
// weighted features, not a one-line string (doc 08, section 7.3, an 8-billion-page
// repository). The repeated body gives the Zipf-like term weighting real prose
// has, which is what stabilizes the signature against a small edit.
const pageText = "the crawler visits a page and extracts links to other pages on the same host while respecting the politeness budget and the freshness model decides when to revisit each page based on how often it changes "

// TestSimhashNearForSimilar checks the graded property: a one-word edit to a
// document moves the simhash by few bits (a near-dup), a full rewrite moves it by
// many (distinct).
func TestSimhashNearForSimilar(t *testing.T) {
	base := strings.Repeat(pageText, 6)
	edit := strings.Replace(base, "politeness budget", "politeness window", 1)
	rewrite := strings.Repeat("astronomers observe distant galaxies through powerful telescopes measuring the redshift of ancient starlight to map the expansion of the universe across billions of years of cosmic history ", 6)

	a := SimhashText(base)
	b := SimhashText(edit)
	c := SimhashText(rewrite)

	if !Near(a, b) {
		t.Errorf("one-word edit not near: distance %d", bits.OnesCount64(a^b))
	}
	if Near(a, c) {
		t.Errorf("a full rewrite was called near: distance %d", bits.OnesCount64(a^c))
	}
}

// TestSimhashIdentical checks that identical text simhashes to the same value, so
// a byte-identical refetch is distance 0.
func TestSimhashIdentical(t *testing.T) {
	s := "meguri turns discovered links into a polite freshness-aware crawl schedule"
	first := SimhashText(s)
	again := SimhashText(s)
	if first != again {
		t.Error("simhash not deterministic")
	}
}

// TestContentFP checks the exact fingerprint: byte-identical bodies share it, any
// change flips it.
func TestContentFP(t *testing.T) {
	a := ContentFP([]byte("hello world"))
	b := ContentFP([]byte("hello world"))
	c := ContentFP([]byte("hello worle"))
	if a != b {
		t.Error("identical bodies have different fingerprints")
	}
	if a == c {
		t.Error("a one-byte change did not flip the fingerprint")
	}
}

// TestNormalizeBody checks whitespace folding so a reflow is not a change.
func TestNormalizeBody(t *testing.T) {
	a := ContentFP(NormalizeBody([]byte("hello   world\n\n\tthere")))
	b := ContentFP(NormalizeBody([]byte("hello world there")))
	if a != b {
		t.Error("whitespace normalization did not collapse a reflow")
	}
}

// TestClassifyChange pins the three verdicts of doc 08 section 7.4, the join with
// the freshness model: byte-identical is no-change, a differing body with a near
// simhash is cosmetic (no meaningful change), a differing body with a far simhash
// is a real change.
func TestClassifyChange(t *testing.T) {
	base := SimhashText(strings.Repeat(pageText, 6))
	cosmetic := SimhashText(strings.Replace(strings.Repeat(pageText, 6), "politeness budget", "politeness window", 1))
	real := SimhashText(strings.Repeat("astronomers observe distant galaxies through powerful telescopes measuring the redshift of ancient starlight to map the expansion of the universe across billions of years ", 6))

	if got := ClassifyChange(100, 100, base, base); got != NoChange {
		t.Errorf("identical fp = %v, want NoChange", got)
	}
	if got := ClassifyChange(100, 200, base, cosmetic); got != CosmeticChange {
		t.Errorf("near simhash, differing fp = %v, want CosmeticChange (distance %d)",
			got, bits.OnesCount64(base^cosmetic))
	}
	if got := ClassifyChange(100, 200, base, real); got != RealChange {
		t.Errorf("far simhash, differing fp = %v, want RealChange (distance %d)",
			got, bits.OnesCount64(base^real))
	}
}

// TestNearThreshold checks the exact k=3 boundary: distance 3 is near, distance 4
// is not.
func TestNearThreshold(t *testing.T) {
	var a uint64 = 0
	near3 := uint64(0b111) // 3 bits set, distance 3
	far4 := uint64(0b1111) // 4 bits set, distance 4
	if !Near(a, near3) {
		t.Error("distance 3 should be near")
	}
	if Near(a, far4) {
		t.Error("distance 4 should not be near")
	}
}
