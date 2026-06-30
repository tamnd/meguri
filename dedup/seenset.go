package dedup

import "github.com/tamnd/meguri"

// Default sizing knobs for a SeenSet. The false-positive rate is a throughput
// knob, not a correctness one: a higher rate means more filter false positives,
// each costing one batched exact confirm, never a dropped page (doc 08, 3.4).
const (
	defaultFPRate   = 0.01    // 1% filter false positives, the spec default
	defaultCapacity = 1 << 20 // initial filter sizing; it is rebuilt as it grows
	defaultBuckets  = 256     // DRUM buckets, by HostKey high byte
)

// SeenSet is the two-tier dedup authority of doc 08 (D5): a resident approximate
// filter in front of an exact key set, so the same URL discovered a thousand
// times becomes one frontier entry. The filter answers "definitely not seen"
// authoritatively and "probably seen" provisionally; a provisional hit is
// confirmed against the exact set, so a false positive costs a confirm, not a
// dropped page, and a false negative never happens because the filter is
// one-sided. This is what makes discovery idempotent (doc 08, section 9.3): a key
// delivered twice creates at most one record.
type SeenSet struct {
	filter *filter
	exact  *exactSet
}

// SeenOption configures a SeenSet.
type SeenOption func(*seenConfig)

type seenConfig struct {
	capacity uint64
	fpRate   float64
	buckets  int
}

// WithCapacity sizes the resident filter for an expected key count.
func WithCapacity(n uint64) SeenOption {
	return func(c *seenConfig) {
		if n > 0 {
			c.capacity = n
		}
	}
}

// WithFPRate sets the filter false-positive budget (the confirm-rate knob).
func WithFPRate(p float64) SeenOption {
	return func(c *seenConfig) {
		if p > 0 && p < 1 {
			c.fpRate = p
		}
	}
}

// WithBuckets sets the number of DRUM buckets the exact set shards into.
func WithBuckets(n int) SeenOption {
	return func(c *seenConfig) {
		if n > 0 {
			c.buckets = n
		}
	}
}

// NewSeenSet builds an empty seen-set.
func NewSeenSet(opts ...SeenOption) *SeenSet {
	c := seenConfig{capacity: defaultCapacity, fpRate: defaultFPRate, buckets: defaultBuckets}
	for _, o := range opts {
		o(&c)
	}
	return &SeenSet{
		filter: newFilter(c.capacity, c.fpRate),
		exact:  newExactSet(c.buckets),
	}
}

// Seen reports whether the key was already in the set, inserting it if new. It is
// the check-and-insert that makes discovery idempotent: true means a rediscovery
// (dedup, no new record), false means a genuinely new URL (now recorded). This is
// onDiscovery's core minus the link-signal crediting (doc 08, section 9.3).
//
// The filter is consulted first. A miss is authoritative, the key is new, so it
// is added to both tiers. A hit is confirmed against the exact set: a true
// positive is a rediscovery, a false positive falls through to the new-URL path.
func (s *SeenSet) Seen(key meguri.URLKey) bool {
	if s.filter.maybeSeen(key) {
		if s.exact.contains(key) {
			return true // confirmed rediscovery
		}
		// Filter false positive: the key is genuinely new.
	}
	s.filter.add(key)
	s.exact.add(key)
	return false
}

// Contains reports membership without inserting, going straight to the exact
// authority through the filter. A filter miss is an authoritative no without
// touching the exact set.
func (s *SeenSet) Contains(key meguri.URLKey) bool {
	if !s.filter.maybeSeen(key) {
		return false
	}
	return s.exact.contains(key)
}

// MaybeContains reports the filter's verdict alone, with no exact confirm. A
// false is authoritative (the one-sided filter never drops a key it has seen),
// a true is the filter's "probably", which on a key the set never held is a
// false positive. It is the probe the benchmark uses to measure the achieved
// filter false-positive rate at the resident bits-per-URL (doc 14, section 3.7);
// the discovery path uses Seen and Contains, which confirm against the exact set.
func (s *SeenSet) MaybeContains(key meguri.URLKey) bool {
	return s.filter.maybeSeen(key)
}

// Merge classifies a whole batch of discovered keys at once through the DRUM
// path, the scale form of Seen: it routes the keys to buckets by HostKey prefix
// and merges each bucket in one sequential pass, returning a Unique verdict per
// key. The filter is updated for every unique key so later single-key Seen calls
// see them. Keys that repeat within the batch dedup against the first occurrence.
func (s *SeenSet) Merge(keys []meguri.URLKey) []Classification {
	batch := make([]pendingKey, len(keys))
	for i, k := range keys {
		batch[i] = pendingKey{key: k, op: opCheckInsert}
	}
	out := s.exact.merge(batch)
	for _, c := range out {
		if c.Unique {
			s.filter.add(c.Key)
		}
	}
	return out
}

// Insert adds a key known to be new (a recovery rebuild from a key column),
// folding it into both tiers without classification.
func (s *SeenSet) Insert(key meguri.URLKey) {
	if !s.filter.maybeSeen(key) || !s.exact.contains(key) {
		s.filter.add(key)
		s.exact.add(key)
	}
}

// InsertBatch folds a batch of keys into both tiers in one DRUM pass per bucket,
// the insert-only scale form of Insert: Merge is to Seen what this is to Insert.
// It routes the keys to buckets by HostKey prefix and merges each bucket's run
// against its sorted run once, so n inserts cost one sorted merge per bucket
// rather than n shifts into the middle of a sorted slice (the O(n^2) the single-
// key add pays at scale, doc 08 section 4.3). The caller owns classification: this
// returns nothing, so it suits a seed loop that has already decided every key is
// new (it deduplicates again internally, so a repeat is harmless, not doubled).
func (s *SeenSet) InsertBatch(keys []meguri.URLKey) {
	if len(keys) == 0 {
		return
	}
	batch := make([]pendingKey, len(keys))
	for i, k := range keys {
		batch[i] = pendingKey{key: k, op: opInsertOnly}
	}
	s.exact.merge(batch)
	for _, k := range keys {
		s.filter.add(k)
	}
}

// Len reports the number of distinct keys the exact set holds.
func (s *SeenSet) Len() int { return s.exact.size }

// BitsPerURL reports the resident filter cost per held key, the budget the gate
// checks against the bits-per-URL table (doc 08, section 3.2).
func (s *SeenSet) BitsPerURL() float64 { return s.filter.bitsPerKey() }
