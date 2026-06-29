package dedup

import (
	"math"

	"github.com/tamnd/meguri"
)

// blockWords is the size of one filter block in 64-bit words: 8 words is 512
// bits, one CPU cache line, so an add or a probe touches exactly one block and
// pays one cache miss (doc 08, section 3.2). This is the blocked-Bloom property
// that matters at a hundred billion keys, where a cache miss is the real cost.
const blockWords = 8

// filter is the resident approximate membership structure: a blocked Bloom
// filter over a partition's URLKeys (doc 08, D5). It is one-sided, the property
// the whole seen-set rests on: maybeSeen never returns false for a key that was
// added, so a false negative never happens and a genuinely new URL is never
// silently dropped. A maybeSeen hit may be a false positive, which the exact set
// confirms; the false-positive rate is the throughput knob, defaulting to 1%.
//
// The spec's steady-state resident filter is a ribbon filter (about 7 bits/url)
// with a blocked-Bloom overlay for recent inserts. meguri ships the blocked-Bloom
// tier first because it is mutable, one-cache-miss, and correct; the ribbon's
// 30% space win is a build-once static refinement that lands with the serialized
// .meguri filter region (doc 10) in the storage milestone. The membership
// contract, one-sided with a confirmable false positive, is identical either way.
type filter struct {
	blocks []uint64 // nBlocks * blockWords words
	nBlock uint64   // number of 512-bit blocks
	k      int      // bits set per key, within its block
	n      uint64   // keys added, for the bits-per-key report
	cap    uint64   // sizing target
}

// newFilter sizes a blocked Bloom filter for capacity keys at the target
// false-positive rate. The bit count follows the classic m = -n*ln(p)/ln(2)^2
// and k = round(m/n*ln2), rounded up to whole 512-bit blocks, with a small
// headroom factor because blocking trades a little false-positive efficiency for
// its one-cache-miss speed (doc 08, section 3.2).
func newFilter(capacity uint64, fpRate float64) *filter {
	if capacity == 0 {
		capacity = 1
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}
	ln2 := math.Ln2
	bitsPerKey := -math.Log(fpRate) / (ln2 * ln2)
	bitsPerKey *= 1.15 // blocking headroom, keeps the realized rate near target
	m := uint64(math.Ceil(float64(capacity) * bitsPerKey))
	nBlock := m/(blockWords*64) + 1
	k := int(math.Round(bitsPerKey * ln2))
	k = max(k, 1)
	k = min(k, 16)
	return &filter{
		blocks: make([]uint64, nBlock*blockWords),
		nBlock: nBlock,
		k:      k,
		cap:    capacity,
	}
}

// hashKey folds the 128-bit URLKey into one 64-bit value. The two halves are
// already xxHash64 outputs (independent host and path hashes), so a mix is
// enough to spread keys across blocks; an odd multiplier decorrelates the halves.
func hashKey(key meguri.URLKey) uint64 {
	h := key.HostKey*0x9E3779B97F4A7C15 + key.PathKey
	h ^= h >> 33
	h *= 0xFF51AFD7ED558CCD
	h ^= h >> 33
	return h
}

// locate returns the block index and the two seed hashes a key's k bit positions
// are derived from by double hashing within the block.
func (f *filter) locate(key meguri.URLKey) (block uint64, h1, h2 uint64) {
	h := hashKey(key)
	block = h % f.nBlock
	h1 = h
	h2 = key.PathKey | 1 // odd stride so the k positions never repeat early
	return
}

func (f *filter) add(key meguri.URLKey) {
	block, h1, h2 := f.locate(key)
	base := block * blockWords
	for i := 0; i < f.k; i++ {
		bit := (h1 + uint64(i)*h2) & 511 // position within the 512-bit block
		f.blocks[base+bit/64] |= 1 << (bit % 64)
	}
	f.n++
}

// maybeSeen reports whether the key might be in the set. False means definitely
// not seen, which is authoritative; true means probably seen, which the exact
// set confirms.
func (f *filter) maybeSeen(key meguri.URLKey) bool {
	block, h1, h2 := f.locate(key)
	base := block * blockWords
	for i := 0; i < f.k; i++ {
		bit := (h1 + uint64(i)*h2) & 511
		if f.blocks[base+bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// bitsPerKey reports the filter's resident cost against the keys it holds, the
// budget the gate checks.
func (f *filter) bitsPerKey() float64 {
	if f.n == 0 {
		return 0
	}
	return float64(len(f.blocks)*64) / float64(f.n)
}

// The residentMembership adapters let a reconstructed blocked-Bloom filter answer
// the same one-sided probe as the ribbon snapshot behind ResidentFilter.
func (f *filter) maybeContains(key meguri.URLKey) bool { return f.maybeSeen(key) }
func (f *filter) bitsPerURL() float64                  { return f.bitsPerKey() }
func (f *filter) length() uint64                       { return f.n }
