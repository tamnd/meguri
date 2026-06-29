package dedup

import (
	"math/bits"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// WeightedFeature is one feature of a document for the simhash: a token and its
// weight, typically a term and its frequency (doc 08, section 7.2).
type WeightedFeature struct {
	Token  string
	Weight int64
}

// Simhash computes the 64-bit Charikar simhash of a document's weighted features
// (doc 08, section 7.2). Each feature is hashed to 64 bits; a 64-entry signed
// accumulator adds the weight where the feature-hash bit is 1 and subtracts it
// where the bit is 0; the simhash bit i is 1 iff accumulator i is positive. The
// Hamming distance between two simhashes tracks the dissimilarity of the two
// documents, which is the graded change signal the freshness model wants.
func Simhash(features []WeightedFeature) uint64 {
	var acc [64]int64
	for _, f := range features {
		h := xxhash.Sum64String(f.Token)
		for i := range 64 {
			if h&(1<<uint(i)) != 0 {
				acc[i] += f.Weight
			} else {
				acc[i] -= f.Weight
			}
		}
	}
	var sig uint64
	for i := range 64 {
		if acc[i] > 0 {
			sig |= 1 << uint(i)
		}
	}
	return sig
}

// SimhashText is the convenience the fetcher uses: tokenize a body into terms,
// weight each by its frequency, and simhash the result. Tokens are maximal runs
// of letters and digits, lowercased, so cosmetic markup and case do not move the
// signature. An empty body simhashes to zero, which the change classifier reads
// as no near-dup signal.
func SimhashText(text string) uint64 {
	tf := make(map[string]int64)
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			tf[b.String()]++
			b.Reset()
		}
	}
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			flush()
		}
	}
	flush()
	if len(tf) == 0 {
		return 0
	}
	features := make([]WeightedFeature, 0, len(tf))
	for tok, w := range tf {
		features = append(features, WeightedFeature{Token: tok, Weight: w})
	}
	return Simhash(features)
}

// nearThreshold is the Hamming distance k=3 operating point Manku, Jain, and Das
// Sarma found right for 64-bit fingerprints at web scale (doc 08, section 7.3):
// at most three of the 64 bits differ for a near-duplicate.
const nearThreshold = 3

// Near reports whether two simhashes are within the near-dup threshold, a
// popcount of their XOR.
func Near(a, b uint64) bool {
	return bits.OnesCount64(a^b) <= nearThreshold
}

// ContentFP is the 64-bit exact content fingerprint: xxHash64 over the body
// (doc 08, section 7.1). Any byte change flips it, so it is the all-or-nothing
// change signal, where the simhash is the graded one. The caller normalizes the
// body (the fetcher computes both once at extraction); NormalizeBody is the
// default normalization.
func ContentFP(body []byte) uint64 {
	return xxhash.Sum64(body)
}

// NormalizeBody collapses runs of whitespace to a single space and trims the
// ends, so trivial reflowing does not move the exact fingerprint. It is the
// minimal normalization the fingerprint runs over; richer boilerplate stripping
// is the fetcher's job before it fills the Outcome.
func NormalizeBody(body []byte) []byte {
	out := make([]byte, 0, len(body))
	var inSpace bool
	for _, c := range body {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' {
			inSpace = true
			continue
		}
		if inSpace && len(out) > 0 {
			out = append(out, ' ')
		}
		inSpace = false
		out = append(out, c)
	}
	return out
}

// ChangeKind is the verdict comparing a fresh fetch against a URL's stored
// signals (doc 08, section 7.4).
type ChangeKind uint8

const (
	// NoChange: the body is byte-identical (content_fp equal). The estimator
	// counts a no-change observation; nochange_streak increments.
	NoChange ChangeKind = iota
	// CosmeticChange: the body differs but the simhash is within Hamming 3, a
	// rotating ad or a live timestamp. It is a no-change for the rate estimator,
	// so it does not poison lambda (doc 08, section 7.5).
	CosmeticChange
	// RealChange: the body differs and the simhash moved more than Hamming 3, a
	// meaningful edit. change_count increments, last_changed is set.
	RealChange
)

// ClassifyChange compares a fresh fetch's content fingerprint and simhash against
// the URL's stored ones and returns the change verdict (doc 08, section 7.4).
// This is the join with the freshness model (doc 06): it defines what "saw a
// change" means for the change-rate estimator, so a cosmetic-only change is a
// no-change observation and lambda reflects the rate the meaningful content moves.
func ClassifyChange(oldFP, newFP, oldSimhash, newSimhash uint64) ChangeKind {
	if oldFP == newFP {
		return NoChange
	}
	if Near(oldSimhash, newSimhash) {
		return CosmeticChange
	}
	return RealChange
}
