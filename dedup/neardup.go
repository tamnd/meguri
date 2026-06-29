package dedup

import (
	"math/bits"
	"sort"

	"github.com/tamnd/meguri"
)

// nearBlocks is the number of permuted tables, k+1 for the Hamming threshold
// k=3 (doc 08, section 7.3). The 64-bit simhash is cut into 4 blocks of 16 bits;
// by the pigeonhole principle, two simhashes within Hamming distance 3 must agree
// exactly on at least one of the 4 blocks, so a query that finds candidates
// sharing each block in turn cannot miss a true near-duplicate.
const nearBlocks = 4

// blockBits is the width of one block: 64 / nearBlocks.
const blockBits = 64 / nearBlocks

// nearEntry is one stored simhash and the URL it belongs to.
type nearEntry struct {
	rotated uint64 // the simhash rotated so this table's block leads
	sim     uint64 // the original simhash
	key     meguri.URLKey
}

// NearDup is the near-duplicate index of doc 08, section 7.3: it answers "is
// there an existing page whose simhash is within Hamming 3 of this one" over a
// growing repository, the mirror-collapse and soft-404 query. It is Manku, Jain,
// and Das Sarma's permuted-table approach: nearBlocks sorted tables, each rotated
// so a different 16-bit block leads, queried by an exact-match probe on the
// leading block followed by a Hamming check on the survivors. This is exact
// recall: a true near-dup is never missed, by the pigeonhole argument above.
type NearDup struct {
	tables [nearBlocks][]nearEntry
	dirty  bool // tables need a re-sort before the next query
}

// NewNearDup returns an empty near-dup index.
func NewNearDup() *NearDup { return &NearDup{} }

// rotateBlock rotates x left so block b (counting from the low end) occupies the
// high blockBits bits, putting that block's bits where a sort orders on them
// first.
func rotateBlock(x uint64, b int) uint64 {
	return bits.RotateLeft64(x, -(b * blockBits))
}

// Add stores a simhash and its key. The tables are marked dirty so the next Find
// re-sorts them; batching inserts keeps the amortized cost low.
func (n *NearDup) Add(sim uint64, key meguri.URLKey) {
	for b := range nearBlocks {
		n.tables[b] = append(n.tables[b], nearEntry{
			rotated: rotateBlock(sim, b),
			sim:     sim,
			key:     key,
		})
	}
	n.dirty = true
}

// Find returns the key of a stored page whose simhash is within Hamming 3 of sim,
// and ok=true, or ok=false when none is near. When several are near, it returns
// the first found, which is enough for the collapse decision.
func (n *NearDup) Find(sim uint64) (meguri.URLKey, bool) {
	n.ensureSorted()
	for b := range nearBlocks {
		table := n.tables[b]
		probe := rotateBlock(sim, b)
		// The leading block is the top blockBits bits; find the range of entries
		// whose leading block equals the probe's, then Hamming-check each.
		hi := probe >> (64 - blockBits)
		lo := sort.Search(len(table), func(i int) bool {
			return table[i].rotated>>(64-blockBits) >= hi
		})
		for i := lo; i < len(table) && table[i].rotated>>(64-blockBits) == hi; i++ {
			if Near(table[i].sim, sim) {
				return table[i].key, true
			}
		}
	}
	return meguri.URLKey{}, false
}

// Len reports the number of distinct simhashes stored.
func (n *NearDup) Len() int { return len(n.tables[0]) }

func (n *NearDup) ensureSorted() {
	if !n.dirty {
		return
	}
	for b := range nearBlocks {
		table := n.tables[b]
		sort.Slice(table, func(i, j int) bool { return table[i].rotated < table[j].rotated })
	}
	n.dirty = false
}
