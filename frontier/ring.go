package frontier

import (
	"math"
	"math/bits"
)

// priorityLevels is the number of front-bank buckets (doc 05, D4). A power of
// two that fits one uint64 occupancy word, so the highest non-empty bucket is
// found with a single leading-zeros count rather than a scan.
const priorityLevels = 64

// priorityBucketScale stretches the logarithmic bucket spacing. A larger scale
// spends more of the 64 levels near priority 1, where the important URLs
// cluster and the scheduler most needs to tell them apart, and leaves the long
// low-priority tail to share the coarse end.
const priorityBucketScale = 4.0

// frontBucket maps a priority in [0,1] to a bucket index, higher priority to a
// higher index. The mapping is logarithmic: bucket = levels-1 at priority 1 and
// falls off as priority halves, so a 0.5 and a 0.4 land in distinct buckets at
// the top while the whole bottom decade collapses into the lowest few. Anything
// at or below 0 lands in bucket 0; anything at or above 1 in the top bucket.
func frontBucket(priority float32) int {
	if priority <= 0 {
		return 0
	}
	if priority >= 1 {
		return priorityLevels - 1
	}
	level := max(int(-math.Log2(float64(priority))*priorityBucketScale), 0)
	if level >= priorityLevels {
		level = priorityLevels - 1
	}
	return priorityLevels - 1 - level
}

// prioRing is the front bank: priorityLevels FIFO buckets fronted by a uint64
// occupancy bitmap (D4). Push drops an item into its priority bucket; pop takes
// the front of the highest non-empty bucket, found in one instruction with
// bits.LeadingZeros64. Items in the same bucket keep insertion order, so a
// dispatch run is deterministic and a checkpoint replays in the same sequence.
//
// It is generic so the same structure serves both fronts the engine keeps: a
// ring of URLKeys for URLs not yet bound to a host queue, and a ring of host
// keys for hosts that are eligible now, ordered by their best URL's priority.
type prioRing[T any] struct {
	buckets [priorityLevels][]T
	occ     uint64
	n       int
}

// len reports how many items the ring holds.
func (r *prioRing[T]) len() int { return r.n }

// push adds item at the bucket for priority.
func (r *prioRing[T]) push(item T, priority float32) {
	b := frontBucket(priority)
	r.buckets[b] = append(r.buckets[b], item)
	r.occ |= 1 << uint(b)
	r.n++
}

// highest returns the index of the highest non-empty bucket, or -1 when empty.
func (r *prioRing[T]) highest() int {
	if r.occ == 0 {
		return -1
	}
	return priorityLevels - 1 - bits.LeadingZeros64(r.occ)
}

// peek returns the next item pop would return, without removing it.
func (r *prioRing[T]) peek() (T, bool) {
	var zero T
	b := r.highest()
	if b < 0 {
		return zero, false
	}
	return r.buckets[b][0], true
}

// pop removes and returns the front of the highest non-empty bucket.
func (r *prioRing[T]) pop() (T, bool) {
	var zero T
	b := r.highest()
	if b < 0 {
		return zero, false
	}
	item := r.buckets[b][0]
	r.buckets[b] = r.buckets[b][1:]
	if len(r.buckets[b]) == 0 {
		r.occ &^= 1 << uint(b)
	}
	r.n--
	return item, true
}
