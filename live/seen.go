package live

import (
	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// seenBuilder collects the distinct URL keys a seal freezes into the seen-set
// filter region. The blocked-Bloom form (dedup.Filter) added keys one at a time as
// the build streamed them, so a seal could size an empty filter up front and Add
// as it went. The ribbon (dedup/ribbon.go) is a static structure solved once over
// the whole key set by Gaussian elimination, so it cannot be fed incrementally: the
// seal collects the keys and builds the filter at Marshal.
//
// The keys arrive in URLKey order on every seal path (the bulk phase-2 merge and the
// compaction merge-join both emit sorted), so a duplicate is always adjacent to its
// twin and is dropped in place. The ribbon needs each key exactly once or its linear
// system is over-determined and the solve fails.
type seenBuilder struct {
	r    int
	keys []m.URLKey
}

// newSeenBuilder sizes an empty collector. hint pre-grows the key slice to the
// expected distinct count so a large seal does not repeatedly reallocate; the fpRate
// picks the ribbon fingerprint width r, the same one-sided false-positive knob the
// blocked-Bloom took (dedup.RibbonBitsForFPR).
func newSeenBuilder(fpRate float64, hint uint64) *seenBuilder {
	return &seenBuilder{
		r:    dedup.RibbonBitsForFPR(fpRate),
		keys: make([]m.URLKey, 0, hint),
	}
}

// addSorted records a key that arrives in nondecreasing URLKey order, dropping an
// exact repeat of the last key. This is the collection point on the seal merge
// loops, where the rows are already key-ordered.
func (s *seenBuilder) addSorted(key m.URLKey) {
	if n := len(s.keys); n > 0 && s.keys[n-1] == key {
		return
	}
	s.keys = append(s.keys, key)
}

// marshal solves the ribbon over the collected keys and returns the seen-set region
// blob plus its realized bits per key, the residency-gate number the build reports.
// An empty key set marshals to an empty ribbon that answers every probe false.
func (s *seenBuilder) marshal() ([]byte, float64, error) {
	blob, err := dedup.BuildRibbonFilter(s.keys, dedup.WithRibbonBits(s.r))
	if err != nil {
		return nil, 0, err
	}
	rf, err := dedup.UnmarshalFilter(blob)
	if err != nil {
		return nil, 0, err
	}
	return blob, rf.BitsPerURL(), nil
}
