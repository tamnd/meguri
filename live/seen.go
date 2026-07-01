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
//
// A large seal partitions its keys into shards at collection so the solve at Marshal
// runs one small ribbon per shard in parallel (dedup/ribbon_sharded.go) instead of
// one linear system over the whole set. Collection-time sharding keeps only one copy
// of the keys, so the shards cost the same memory a single key slice would; a key
// routes to its shard by dedup.RibbonShardIndex, and since equal keys hash alike they
// land in the same shard adjacent to each other, so the in-place duplicate drop still
// works per shard. The shard count comes from the seal's size hint, which every seal
// path knows, and the filter blob records it so the query path routes identically.
type seenBuilder struct {
	r      int
	shards [][]m.URLKey
}

// newSeenBuilder sizes an empty collector. hint is the expected distinct key count: it
// picks the shard count (dedup.RibbonShardCount) and pre-grows each shard slice to its
// share of the hint so a large seal does not repeatedly reallocate. The fpRate picks
// the ribbon fingerprint width r, the same one-sided false-positive knob the
// blocked-Bloom took (dedup.RibbonBitsForFPR).
func newSeenBuilder(fpRate float64, hint uint64) *seenBuilder {
	shardCount := dedup.RibbonShardCount(int(hint))
	shards := make([][]m.URLKey, shardCount)
	// Per-shard headroom over the mean so ordinary Poisson spread across shards does
	// not trigger a synchronized reallocation as they all fill together.
	per := int(hint)/shardCount + int(hint)/(shardCount*16) + 64
	for i := range shards {
		shards[i] = make([]m.URLKey, 0, per)
	}
	return &seenBuilder{
		r:      dedup.RibbonBitsForFPR(fpRate),
		shards: shards,
	}
}

// addSorted records a key that arrives in nondecreasing URLKey order, routing it to
// its shard and dropping an exact repeat of that shard's last key. Equal keys hash to
// the same shard and arrive adjacent, so the per-shard tail check drops the duplicate
// the ribbon solve cannot take twice. This is the collection point on the seal merge
// loops, where the rows are already key-ordered.
func (s *seenBuilder) addSorted(key m.URLKey) {
	idx := dedup.RibbonShardIndex(key, len(s.shards))
	sh := s.shards[idx]
	if n := len(sh); n > 0 && sh[n-1] == key {
		return
	}
	s.shards[idx] = append(sh, key)
}

// marshal solves the ribbon over the collected keys and returns the seen-set region
// blob plus its realized bits per key, the residency-gate number the build reports.
// With one shard this is the single kind-1 ribbon; with more it is the kind-2 sharded
// ribbon solved in parallel. An empty key set marshals to an empty ribbon that answers
// every probe false.
func (s *seenBuilder) marshal() ([]byte, float64, error) {
	blob, err := dedup.BuildShardedRibbonFilter(s.shards, dedup.WithRibbonBits(s.r))
	if err != nil {
		return nil, 0, err
	}
	rf, err := dedup.UnmarshalFilter(blob)
	if err != nil {
		return nil, 0, err
	}
	return blob, rf.BitsPerURL(), nil
}
