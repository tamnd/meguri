package dedup

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"

	"github.com/tamnd/meguri"
)

// The ribbon filter is the cold, read-mostly form of the seen-set's approximate
// tier (doc 08, section 3.2): a static one-sided membership filter that holds the
// same keys as the blocked-Bloom filter at about 7 bits per url instead of 11,
// the 30% space win the spec calls for. It is build-once: a frozen snapshot
// solves a banded linear system over the keys, and a query recomputes a few words
// and compares, so it never inserts. The membership contract is identical to the
// blocked-Bloom filter, one-sided: a key that was built into the filter always
// matches (no false negative, so a genuinely new url is never dropped), and a key
// that was not matches with probability 2^-r, the confirmable false positive the
// exact set settles.
//
// The structure is a standard ribbon (Dillinger and Walzer): m slots of r bits,
// each key mapped to a width-64 band of slots starting at a hashed position with
// a hashed coefficient row and an r-bit fingerprint. Construction is incremental
// Gaussian elimination over GF(2) with the band kept on the diagonal; a rare
// unsolvable system is retried with a new hash seed, and the table is grown if a
// run of seeds all fail. Back-substitution fills the slot values so that every
// built key's band XORs to its fingerprint, which is what makes the filter
// one-sided.

const (
	ribbonWidth      = 64   // band width in slots, one uint64 coefficient row
	defaultRibbonR   = 7    // fingerprint bits, ~7 bits/url and a 2^-7 false-positive rate
	ribbonLoad       = 0.90 // keys per slot target; m = ceil(n/load), the space-vs-solve knob
	maxRibbonSeeds   = 64   // hash reseeds tried before growing the table
	maxRibbonGrows   = 4    // table growth steps before giving up
	maxRibbonRBits   = 16   // a fingerprint fits in a uint16 slot
	ribbonHeaderSize = 24   // version, kind, r, reserved, seed u32, n u64, m u64
)

// errRibbonBuild is returned when the banded system stays unsolvable after every
// reseed and growth step, which on real key sets effectively never happens.
var errRibbonBuild = errors.New("dedup: ribbon filter construction failed")

// ribbon is the solved static filter: m slot values of r bits, plus the seed and
// key count the query path needs. It answers maybeContains and reports its cost,
// but it does not insert.
type ribbon struct {
	z     []uint16 // m slot values, each holding an r-bit fingerprint contribution
	m     int
	rbits uint
	seed  uint32
	n     uint64
}

// ribbonMix is a 64-bit avalanche finalizer (the splitmix64 / Murmur mixer),
// enough to spread a key's two hash halves across the band, the fingerprint, and
// the start position.
func ribbonMix(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xFF51AFD7ED558CCD
	x ^= x >> 33
	x *= 0xC4CEB9FE1A85EC53
	x ^= x >> 33
	return x
}

// ribbonHash folds a 128-bit URLKey and a seed into two decorrelated 64-bit
// words. The two halves are already independent xxHash64 outputs, so seeding and
// mixing is enough; the seed lets construction reshuffle every key's placement on
// a reseed without touching the keys.
func ribbonHash(key meguri.URLKey, seed uint32) (a, b uint64) {
	s := uint64(seed)*0x9E3779B97F4A7C15 + 0x165667B19E3779F9
	a = ribbonMix(key.HostKey ^ s ^ (key.PathKey * 0xD6E8FEB86659FD93))
	b = ribbonMix(key.PathKey ^ (s * 0xC2B2AE3D27D4EB4F) ^ (key.HostKey * 0x9E3779B97F4A7C15))
	return
}

// ribbonDerive maps a key to its band: the start slot, the width-64 coefficient
// row (low bit forced set so the leading coefficient sits at start), and the
// r-bit fingerprint. Build and query must derive identically, so this is the one
// place the mapping lives.
func ribbonDerive(key meguri.URLKey, seed uint32, m int, rbits uint) (start int, coeff uint64, fp uint16) {
	a, b := ribbonHash(key, seed)
	startRange := uint64(m - ribbonWidth + 1)
	start = int(a % startRange)
	coeff = b | 1
	fp = uint16(ribbonMix(a^(b<<1)) >> (64 - rbits))
	return
}

// solveRibbon attempts to build the slot values for keys at a fixed table size m
// and seed. It returns ok=false when the banded system is inconsistent or a row's
// support runs past the table, the signal to reseed or grow.
func solveRibbon(keys []meguri.URLKey, rbits uint, seed uint32, m int) (z []uint16, ok bool) {
	rows := make([]uint64, m) // rows[i]: coefficient row pivoted at slot i, low bit set when occupied
	rhs := make([]uint16, m)  // rhs[i]: the fingerprint accumulated at pivot i
	for _, key := range keys {
		start, coeff, fp := ribbonDerive(key, seed, m, rbits)
		i := start
		cr := coeff
		b := fp
		for {
			if cr == 0 {
				if b != 0 {
					return nil, false // inconsistent equation
				}
				break // redundant equation, consistent
			}
			j := bits.TrailingZeros64(cr)
			i += j
			cr >>= uint(j)
			if i >= m {
				return nil, false // band ran past the table
			}
			if rows[i] == 0 {
				rows[i] = cr
				rhs[i] = b
				break
			}
			cr ^= rows[i] // cancels the pivot bit
			b ^= rhs[i]
			cr >>= 1
			i++
		}
	}
	// Back-substitute: a slot's value is its pivot fingerprint XOR the values of
	// the slots its row still references, filled from the high slots down.
	z = make([]uint16, m)
	for i := m - 1; i >= 0; i-- {
		if rows[i] == 0 {
			continue
		}
		v := rhs[i]
		mask := rows[i] & ^uint64(1)
		for mask != 0 {
			k := bits.TrailingZeros64(mask)
			if p := i + k; p < m {
				v ^= z[p]
			}
			mask &= mask - 1
		}
		z[i] = v
	}
	return z, true
}

// buildRibbon solves the filter over keys, reseeding on a failed system and
// growing the table after a run of failed seeds. On real key sets the first seed
// almost always solves; the growth loop is the backstop a pathological set needs.
func buildRibbon(keys []meguri.URLKey, rbits uint) (*ribbon, error) {
	n := len(keys)
	m := int(math.Ceil(float64(n) / ribbonLoad))
	m = max(m, ribbonWidth*2) // floor so the band fits and the start range is positive
	for range maxRibbonGrows {
		for seed := range uint32(maxRibbonSeeds) {
			if z, ok := solveRibbon(keys, rbits, seed, m); ok {
				return &ribbon{z: z, m: m, rbits: rbits, seed: seed, n: uint64(n)}, nil
			}
		}
		m += m/20 + ribbonWidth // grow about 5% and retry the seeds
	}
	return nil, errRibbonBuild
}

// query is the one-sided probe: it XORs the key's band of slot values and
// compares to the key's fingerprint. A built key always matches; a key never
// built matches with probability 2^-r.
func (rb *ribbon) query(key meguri.URLKey) bool {
	start, coeff, fp := ribbonDerive(key, rb.seed, rb.m, rb.rbits)
	var v uint16
	mask := coeff
	for mask != 0 {
		j := bits.TrailingZeros64(mask)
		if p := start + j; p < rb.m {
			v ^= rb.z[p]
		}
		mask &= mask - 1
	}
	return v == fp
}

func (rb *ribbon) maybeContains(key meguri.URLKey) bool { return rb.query(key) }

func (rb *ribbon) bitsPerURL() float64 {
	if rb.n == 0 {
		return 0
	}
	return float64(rb.m) * float64(rb.rbits) / float64(rb.n)
}

func (rb *ribbon) length() uint64 { return rb.n }

// marshal serializes the ribbon to the kind-1 filter blob: the fixed header then
// the slot values bit-packed at exactly r bits each, so the on-disk cost tracks
// the resident bits-per-url rather than rounding every slot up to a byte.
func (rb *ribbon) marshal() []byte {
	body := packBits(rb.z, rb.rbits)
	out := make([]byte, ribbonHeaderSize, ribbonHeaderSize+len(body))
	out[0] = filterBlobVersion
	out[1] = filterKindRibbon
	out[2] = uint8(rb.rbits)
	// out[3] reserved.
	binary.LittleEndian.PutUint32(out[4:8], rb.seed)
	binary.LittleEndian.PutUint64(out[8:16], rb.n)
	binary.LittleEndian.PutUint64(out[16:24], uint64(rb.m))
	out = append(out, body...)
	return out
}

// unmarshalRibbon reconstructs a ribbon from the kind-1 blob, rebuilding the same
// slot values so the query answers identically to the original.
func unmarshalRibbon(b []byte) (*ribbon, error) {
	if len(b) < ribbonHeaderSize {
		return nil, errBadFilterBlob
	}
	rbits := uint(b[2])
	if rbits == 0 || rbits > maxRibbonRBits {
		return nil, errBadFilterBlob
	}
	seed := binary.LittleEndian.Uint32(b[4:8])
	n := binary.LittleEndian.Uint64(b[8:16])
	m := int(binary.LittleEndian.Uint64(b[16:24]))
	if m <= 0 || m < ribbonWidth {
		return nil, errBadFilterBlob
	}
	body := b[ribbonHeaderSize:]
	if len(body) != (m*int(rbits)+7)/8 {
		return nil, errBadFilterBlob
	}
	z := unpackBits(body, m, rbits)
	return &ribbon{z: z, m: m, rbits: rbits, seed: seed, n: n}, nil
}

// packBits writes len(z) values of rbits bits each, least-significant bit first,
// into a tight byte slice.
func packBits(z []uint16, rbits uint) []byte {
	mask := uint64(1)<<rbits - 1
	out := make([]byte, (len(z)*int(rbits)+7)/8)
	var acc uint64
	var nbits uint
	var bi int
	for _, v := range z {
		acc |= (uint64(v) & mask) << nbits
		nbits += rbits
		for nbits >= 8 {
			out[bi] = byte(acc)
			bi++
			acc >>= 8
			nbits -= 8
		}
	}
	if nbits > 0 {
		out[bi] = byte(acc)
	}
	return out
}

// unpackBits reverses packBits, reading count values of rbits bits each.
func unpackBits(b []byte, count int, rbits uint) []uint16 {
	mask := uint64(1)<<rbits - 1
	out := make([]uint16, count)
	var acc uint64
	var nbits uint
	var bi int
	for i := range out {
		for nbits < rbits {
			if bi < len(b) {
				acc |= uint64(b[bi]) << nbits
				bi++
			}
			nbits += 8
		}
		out[i] = uint16(acc & mask)
		acc >>= rbits
		nbits -= rbits
	}
	return out
}

// RibbonOption configures a ribbon snapshot build.
type RibbonOption func(*ribbonConfig)

type ribbonConfig struct {
	rbits uint
}

// WithRibbonBits sets the fingerprint width in bits, the bits-per-url versus
// false-positive-rate knob (a false-positive rate of 2^-bits). It is clamped to
// the 1..16 a uint16 slot holds.
func WithRibbonBits(bits int) RibbonOption {
	return func(c *ribbonConfig) {
		if bits >= 1 && bits <= maxRibbonRBits {
			c.rbits = uint(bits)
		}
	}
}

// BuildRibbonFilter builds the cold ribbon form over a key set and returns its
// serialized kind-1 blob, the static snapshot a frozen partition carries instead
// of the mutable blocked-Bloom filter. UnmarshalFilter reads it back into a
// ResidentFilter that answers MaybeContains behind the same one-sided contract.
func BuildRibbonFilter(keys []meguri.URLKey, opts ...RibbonOption) ([]byte, error) {
	c := ribbonConfig{rbits: defaultRibbonR}
	for _, o := range opts {
		o(&c)
	}
	rb, err := buildRibbon(keys, c.rbits)
	if err != nil {
		return nil, err
	}
	return rb.marshal(), nil
}

// MarshalRibbon freezes the seen-set's exact keys into a ribbon snapshot blob,
// the read-mostly form an engine writes when a partition goes cold. The keys come
// from the exact set, so the snapshot holds exactly what the set holds.
func (s *SeenSet) MarshalRibbon(opts ...RibbonOption) ([]byte, error) {
	return BuildRibbonFilter(s.exact.keys(), opts...)
}
