package meguri

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// URLKey is the 128-bit identity of a URL in the frontier (D3). The HostKey is
// the high 64 bits and the PathKey is the low 64 bits. The HostKey is at once
// the partition key, the politeness key, and the colocation key, so a host's
// URLs always live together: same partition, same politeness bucket, contiguous
// in the file.
type URLKey struct {
	HostKey uint64
	PathKey uint64
}

// Less reports whether a sorts before b when the 128-bit key is read as a
// big-endian integer: HostKey first, then PathKey. This is the sort order of
// the URL table (doc 10), and it is what puts a host's rows in one contiguous
// range. Note the order is logical, not the little-endian byte image the file
// stores each half in.
func (a URLKey) Less(b URLKey) bool {
	if a.HostKey != b.HostKey {
		return a.HostKey < b.HostKey
	}
	return a.PathKey < b.PathKey
}

// Compare returns -1, 0, or 1 as a sorts before, equal to, or after b, in the
// same big-endian 128-bit order as Less.
func (a URLKey) Compare(b URLKey) int {
	switch {
	case a.HostKey < b.HostKey:
		return -1
	case a.HostKey > b.HostKey:
		return 1
	case a.PathKey < b.PathKey:
		return -1
	case a.PathKey > b.PathKey:
		return 1
	default:
		return 0
	}
}

// Bytes returns the 16-byte on-the-wire form: HostKey then PathKey, each
// little-endian, matching the file's URLKey field layout (doc 10). The two
// halves read back as the two u64s with binary.LittleEndian.
func (k URLKey) Bytes() [16]byte {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], k.HostKey)
	binary.LittleEndian.PutUint64(b[8:16], k.PathKey)
	return b
}

// URLKeyFromBytes decodes the 16-byte form Bytes produces.
func URLKeyFromBytes(b [16]byte) URLKey {
	return URLKey{
		HostKey: binary.LittleEndian.Uint64(b[0:8]),
		PathKey: binary.LittleEndian.Uint64(b[8:16]),
	}
}

// pathSeed keeps the PathKey independent of the HostKey: the host half hashes
// with xxHash64 seed 0 (the fleet convention), the path half mixes this seed in
// first, so the same path under two hosts does not collide the low halves.
const pathSeed uint64 = 0x9E3779B97F4A7C15

// HostKeyOf hashes a host grouping string (a registrable domain or a full host,
// per the host's HostGrouping) into the 64-bit HostKey. The hash is xxHash64
// with seed 0, the fleet convention, so the same grouping always maps to the
// same partition.
//
// The caller is responsible for producing the canonical grouping string;
// canonicalization (the CanonPolicy and Public Suffix List rules of doc 03)
// lands with the seen-set in M2. Until then this is the stable derivation the
// format and the router agree on.
func HostKeyOf(grouping string) uint64 {
	return xxhash.Sum64String(grouping)
}

// PathKeyOf hashes the path-and-query remainder of a canonical URL into the
// 64-bit PathKey. The seed differs from the host seed so the two halves are
// independent.
func PathKeyOf(pathAndQuery string) uint64 {
	h := xxhash.New()
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], pathSeed)
	_, _ = h.Write(seed[:])
	_, _ = h.WriteString(pathAndQuery)
	return h.Sum64()
}

// MakeURLKey builds a URLKey from a host grouping string and the path-and-query
// remainder. It is the convenience the discovery path uses once a URL is
// canonicalized.
func MakeURLKey(grouping, pathAndQuery string) URLKey {
	return URLKey{
		HostKey: HostKeyOf(grouping),
		PathKey: PathKeyOf(pathAndQuery),
	}
}
