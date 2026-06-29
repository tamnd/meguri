package dedup

import (
	"sort"

	"github.com/tamnd/meguri"
)

// pendingOp distinguishes the two reasons a key enters a DRUM batch: a discovery
// checks-and-inserts (classify duplicate or unique), a recovery insert-only
// rebuilds the set from a known-good key column without reclassifying.
type pendingOp uint8

const (
	opCheckInsert pendingOp = iota // discovery: classify, then insert if unique
	opInsertOnly                   // recovery: fold the key in, no classification
)

// pendingKey is one accumulated check or insert, sorted by URLKey before a merge
// so the batch sweeps the sorted on-disk bucket in a single linear pass.
type pendingKey struct {
	key meguri.URLKey
	op  pendingOp
}

// Classification is the verdict the DRUM merge returns for one checked key: a
// rediscovery (Unique false, the key was already present) or a genuinely new URL
// (Unique true, the key was folded into the bucket).
type Classification struct {
	Key    meguri.URLKey
	Unique bool
}

// exactSet is the exact on-disk authority of the seen-set (doc 08, D5), the
// truth a filter hit is confirmed against. It is sharded into buckets by the high
// bits of the HostKey, so a host group's keys cluster in one bucket and stay
// contiguous, the same colocation the URL table's sort gives. Each bucket is a
// sorted run of URLKeys.
//
// In M2 the buckets are resident sorted slices, the in-memory form of the sorted
// runs doc 11 lands on disk and doc 10 serializes as the .meguri urlkey column.
// The DRUM discipline is the same at either temperature: batch the checks, route
// them to buckets by HostKey prefix, and merge each batch against its bucket in
// one sequential pass, so the random-lookup dedup of a billion-scale crawl
// becomes sequential IO (the IRLbot win, doc 08 section 4).
type exactSet struct {
	buckets [][]meguri.URLKey
	shift   uint // 64 - log2(len(buckets)), the HostKey prefix width
	size    int
}

// newExactSet builds an exact set with nBuckets buckets, rounded up to a power of
// two so the bucket index is a shift of the HostKey.
func newExactSet(nBuckets int) *exactSet {
	n := 1
	bits := uint(0)
	for n < nBuckets {
		n <<= 1
		bits++
	}
	return &exactSet{
		buckets: make([][]meguri.URLKey, n),
		shift:   64 - bits,
	}
}

// bucketOf routes a key to its bucket by the high bits of the HostKey, so a host
// group lands in exactly one bucket (doc 08, section 9.2).
func (s *exactSet) bucketOf(key meguri.URLKey) int {
	if s.shift >= 64 {
		return 0 // a single bucket
	}
	return int(key.HostKey >> s.shift)
}

// keys returns every key in the set, concatenating the per-bucket sorted runs.
// It is the source a ribbon snapshot freezes from; the order does not matter to
// the ribbon build.
func (s *exactSet) keys() []meguri.URLKey {
	out := make([]meguri.URLKey, 0, s.size)
	for _, b := range s.buckets {
		out = append(out, b...)
	}
	return out
}

// contains reports whether the key is present, by binary search of its sorted
// bucket. This is the confirm a filter hit triggers; doc 11 makes it a row-group
// scan against the .meguri urlkey column.
func (s *exactSet) contains(key meguri.URLKey) bool {
	b := s.buckets[s.bucketOf(key)]
	i := sort.Search(len(b), func(i int) bool { return !b[i].Less(key) })
	return i < len(b) && b[i] == key
}

// add inserts a key directly, keeping its bucket sorted. It is the single-key
// path; the batched merge is the scale path.
func (s *exactSet) add(key meguri.URLKey) bool {
	idx := s.bucketOf(key)
	b := s.buckets[idx]
	i := sort.Search(len(b), func(i int) bool { return !b[i].Less(key) })
	if i < len(b) && b[i] == key {
		return false // already present
	}
	b = append(b, meguri.URLKey{})
	copy(b[i+1:], b[i:])
	b[i] = key
	s.buckets[idx] = b
	s.size++
	return true
}

// merge classifies a whole batch against the on-disk buckets in one sequential
// pass each (doc 08, section 4.3, mergeBucket written out). The batch is routed to
// per-bucket runs, each run sorted once and two-pointer merged against its sorted
// bucket: a key present on disk is a duplicate, a key absent is unique and folded
// into the bucket's sorted run. No per-key random seek.
func (s *exactSet) merge(batch []pendingKey) []Classification {
	if len(batch) == 0 {
		return nil
	}
	// Route the batch to per-bucket runs.
	byBucket := make(map[int][]pendingKey)
	for _, pk := range batch {
		idx := s.bucketOf(pk.key)
		byBucket[idx] = append(byBucket[idx], pk)
	}

	out := make([]Classification, 0, len(batch))
	for idx, run := range byBucket {
		out = s.mergeBucket(idx, run, out)
	}
	return out
}

// mergeBucket is the DRUM core for one bucket: sort the run, two-pointer it
// against the sorted bucket, classify each checked key, and fold the unique keys
// back in as a merged sorted run.
func (s *exactSet) mergeBucket(idx int, run []pendingKey, out []Classification) []Classification {
	sort.Slice(run, func(i, j int) bool {
		if run[i].key == run[j].key {
			return run[i].op < run[j].op
		}
		return run[i].key.Less(run[j].key)
	})

	disk := s.buckets[idx]
	var newKeys []meguri.URLKey
	di := 0
	var lastNew meguri.URLKey
	var haveNew bool
	for i := range run {
		pk := run[i]
		// Advance the disk pointer past keys the batch skips.
		for di < len(disk) && disk[di].Less(pk.key) {
			di++
		}
		onDisk := di < len(disk) && disk[di] == pk.key
		// A key may repeat within the batch (the same URL discovered twice in
		// one window); the first occurrence decides, the rest dedup against the
		// new keys accumulated so far.
		dupInBatch := haveNew && lastNew == pk.key
		if onDisk || dupInBatch {
			if pk.op == opCheckInsert {
				out = append(out, Classification{Key: pk.key, Unique: false})
			}
			continue
		}
		if pk.op == opCheckInsert {
			out = append(out, Classification{Key: pk.key, Unique: true})
		}
		newKeys = append(newKeys, pk.key)
		lastNew, haveNew = pk.key, true
	}

	if len(newKeys) > 0 {
		s.buckets[idx] = mergeSortedRuns(disk, newKeys)
		s.size += len(newKeys)
	}
	return out
}

// mergeSortedRuns folds a sorted run of new keys into a sorted bucket, the
// append-and-merge that keeps a bucket a sorted run without a random write into
// its middle (doc 08, section 4.3, step 4).
func mergeSortedRuns(a, b []meguri.URLKey) []meguri.URLKey {
	out := make([]meguri.URLKey, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Less(b[j]) {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}
