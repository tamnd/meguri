package dedup

import (
	"encoding/binary"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/tamnd/meguri"
)

// The sharded ribbon splits one seal's keys across independent ribbon sub-filters so
// a large seal solves in parallel instead of grinding one goroutine through a single
// linear system. A key routes to exactly one shard by a fixed shard hash, so the
// one-sided contract still holds unchanged: a built key lands in its shard and that
// shard's ribbon always matches it (no false negative), and a shard realizes the same
// 2^-r false-positive rate as a single ribbon, so the whole filter does too.
//
// Sharding buys three things a single ribbon at 100M keys does not have. Each solve
// covers a few hundred thousand keys, well inside the load 0.85 threshold where the
// first seed solves, so a reseed re-solves one small shard rather than the whole set.
// The solves run across every core instead of one, turning the seal from a single
// long tail into a parallel pass. And each solve's scratch is a few megabytes that is
// reclaimed as the shard finishes, so a 100M seal fits a small box's memory instead
// of holding one table proportional to the full key count.
//
// The shard count is a power of two chosen from the key count so a shard holds on the
// order of ribbonShardTarget keys. A key routes by the high bits of a shard hash that
// is decorrelated from the band hash, so placement across shards and placement inside
// a shard are independent and reseeding a shard never moves a key out of it.

const (
	ribbonShardTarget       = 1 << 18 // ~262k keys per shard, inside the first-seed solve region
	maxRibbonShards         = 1 << 12 // 4096 shards caps the per-seal fan-out
	shardedRibbonHeaderSize = 16      // version, kind, 2 reserved, u32 shards, u64 n
)

// RibbonShardCount picks the power-of-two shard count for a seal of n keys: one shard
// when n is at or below the target so small seals stay a single ribbon, otherwise the
// smallest power of two that keeps each shard near the target, capped at
// maxRibbonShards. A seal path shards its keys at collection with this count and the
// blob records it, so the query path routes with the same count.
func RibbonShardCount(n int) int {
	if n <= ribbonShardTarget {
		return 1
	}
	s := 1
	for s < maxRibbonShards && s*ribbonShardTarget < n {
		s <<= 1
	}
	return s
}

// ribbonShardHash folds a key into the shard-routing word with constants distinct
// from ribbonHash, so which shard a key lands in is independent of where it lands
// inside that shard's band.
func ribbonShardHash(key meguri.URLKey) uint64 {
	return ribbonMix(key.HostKey*0xD1B54A32D192ED03 ^ key.PathKey*0x2545F4914F6CDD1D ^ 0x9E3779B97F4A7C15)
}

// RibbonShardIndex routes a key to one of shards (which must be a power of two) by the
// high log2(shards) bits of its shard hash, so the split is even and the index is a
// single shift. It is the one place the routing lives; a seal and its query must call
// it with the same shard count.
func RibbonShardIndex(key meguri.URLKey, shards int) int {
	if shards <= 1 {
		return 0
	}
	lg := uint(bits.TrailingZeros(uint(shards)))
	return int(ribbonShardHash(key) >> (64 - lg))
}

// shardedRibbon is a set of independent ribbon sub-filters, one per shard, that
// answers the same one-sided membership probe as a single ribbon by routing the key
// to its shard first.
type shardedRibbon struct {
	shards []*ribbon
	n      uint64
}

func (sr *shardedRibbon) maybeContains(key meguri.URLKey) bool {
	return sr.shards[RibbonShardIndex(key, len(sr.shards))].query(key)
}

func (sr *shardedRibbon) length() uint64 { return sr.n }

// bitsPerURL sums every shard's slot cost over the total key count, so it reports the
// filter's real resident bits per url including the per-shard floor overhead.
func (sr *shardedRibbon) bitsPerURL() float64 {
	if sr.n == 0 {
		return 0
	}
	var slotBits uint64
	for _, rb := range sr.shards {
		slotBits += uint64(rb.m) * uint64(rb.rbits)
	}
	return float64(slotBits) / float64(sr.n)
}

// buildShardedRibbon solves one ribbon per shard slice in parallel and returns the
// combined filter. Each shard slice already holds its own keys, partitioned by
// RibbonShardIndex and deduplicated by the caller. Workers are capped at the core
// count so a many-shard seal saturates the box without oversubscribing it, and each
// worker builds its own shard scratch inside buildRibbon so the solves do not
// contend.
func buildShardedRibbon(shardKeys [][]meguri.URLKey, rbits uint, n uint64) (*shardedRibbon, error) {
	shards := make([]*ribbon, len(shardKeys))
	workers := min(runtime.GOMAXPROCS(0), len(shardKeys))
	workers = max(workers, 1)

	var (
		next     atomic.Int64
		wg       sync.WaitGroup
		errOnce  sync.Once
		buildErr error
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(shardKeys) {
					return
				}
				rb, err := buildRibbon(shardKeys[i], rbits)
				if err != nil {
					errOnce.Do(func() { buildErr = err })
					return
				}
				shards[i] = rb
			}
		}()
	}
	wg.Wait()
	if buildErr != nil {
		return nil, buildErr
	}
	return &shardedRibbon{shards: shards, n: n}, nil
}

// marshal serializes the sharded ribbon to the kind-2 filter blob: the fixed header,
// a u32 length index of the S per-shard blobs, then the shard blobs concatenated.
// Each shard blob is a full kind-1 ribbon blob, so a shard round-trips through the
// same unmarshalRibbon the single form uses.
func (sr *shardedRibbon) marshal() []byte {
	subs := make([][]byte, len(sr.shards))
	total := shardedRibbonHeaderSize + 4*len(sr.shards)
	for i, rb := range sr.shards {
		subs[i] = rb.marshal()
		total += len(subs[i])
	}
	out := make([]byte, shardedRibbonHeaderSize, total)
	out[0] = filterBlobVersion
	out[1] = filterKindShardedRibbon
	// out[2], out[3] reserved.
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(sr.shards)))
	binary.LittleEndian.PutUint64(out[8:16], sr.n)
	for _, s := range subs {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(s)))
	}
	for _, s := range subs {
		out = append(out, s...)
	}
	return out
}

// unmarshalShardedRibbon reconstructs the sharded ribbon from its kind-2 blob,
// rebuilding each shard through unmarshalRibbon so the query answers identically to
// the original for every key.
func unmarshalShardedRibbon(b []byte) (*shardedRibbon, error) {
	if len(b) < shardedRibbonHeaderSize {
		return nil, errBadFilterBlob
	}
	s := int(binary.LittleEndian.Uint32(b[4:8]))
	n := binary.LittleEndian.Uint64(b[8:16])
	if s <= 0 || s > maxRibbonShards {
		return nil, errBadFilterBlob
	}
	idx := b[shardedRibbonHeaderSize:]
	if len(idx) < 4*s {
		return nil, errBadFilterBlob
	}
	lens := make([]int, s)
	var bodyLen int
	for i := range lens {
		l := int(binary.LittleEndian.Uint32(idx[i*4:]))
		lens[i] = l
		bodyLen += l
	}
	body := idx[4*s:]
	if len(body) != bodyLen {
		return nil, errBadFilterBlob
	}
	shards := make([]*ribbon, s)
	off := 0
	for i, l := range lens {
		rb, err := unmarshalRibbon(body[off : off+l])
		if err != nil {
			return nil, err
		}
		shards[i] = rb
		off += l
	}
	return &shardedRibbon{shards: shards, n: n}, nil
}

// BuildShardedRibbonFilter builds the cold ribbon form over keys already partitioned
// into shards (by RibbonShardIndex with count len(shardKeys)) and returns the
// serialized blob a frozen partition carries. A single shard emits the kind-1 blob
// BuildRibbonFilter would, so small seals stay byte-for-byte compatible with the
// single form; more than one shard emits the kind-2 sharded blob. UnmarshalFilter
// reads either back into a ResidentFilter behind the same one-sided contract.
func BuildShardedRibbonFilter(shardKeys [][]meguri.URLKey, opts ...RibbonOption) ([]byte, error) {
	c := ribbonConfig{rbits: defaultRibbonR}
	for _, o := range opts {
		o(&c)
	}
	if len(shardKeys) <= 1 {
		var keys []meguri.URLKey
		if len(shardKeys) == 1 {
			keys = shardKeys[0]
		}
		rb, err := buildRibbon(keys, c.rbits)
		if err != nil {
			return nil, err
		}
		return rb.marshal(), nil
	}
	var n uint64
	for _, ks := range shardKeys {
		n += uint64(len(ks))
	}
	sr, err := buildShardedRibbon(shardKeys, c.rbits, n)
	if err != nil {
		return nil, err
	}
	return sr.marshal(), nil
}
