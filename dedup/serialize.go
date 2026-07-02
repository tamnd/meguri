package dedup

import (
	"encoding/binary"
	"errors"

	"github.com/tamnd/meguri"
)

// This file serializes the resident blocked-Bloom filter to a portable byte blob
// and back (doc 08, D5; doc 10 section 6, the seen-set filter region). The exact
// set is rebuilt from the on-disk urlkey column on reload, but the filter is pure
// derived state that would otherwise cost one add per key to rebuild, so a
// snapshot carries it across a checkpoint. The blob is self-describing: a reader
// reconstructs the exact same bit array and answers MaybeContains identically,
// which is the round-trip the format gate checks.
//
// Blob layout, little-endian, the fleet byte order:
//
//	u8  version (1)
//	u8  kind    (filterKindBlockedBloom)
//	u8  k       (bits set per key)
//	u8  reserved (0)
//	u32 reserved (0, keeps the header 8-byte aligned for the u64 body)
//	u64 nBlock  (number of 512-bit blocks)
//	u64 n       (keys added, for the bits-per-url report)
//	u64 cap     (the sizing target the filter was built for)
//	[nBlock*blockWords] u64 blocks
//
// kind 0 is the blocked-Bloom filter meguri ships today; kind 1 is reserved for
// the ribbon static form (doc 08, section 3.2), which serializes through the same
// region behind the same one-sided contract.
const (
	filterBlobVersion       uint8 = 1
	filterKindBlockedBloom  uint8 = 0
	filterKindRibbon        uint8 = 1 // single static ribbon, one linear system
	filterKindShardedRibbon uint8 = 2 // ribbon split into independent per-shard systems
	filterBlobHeaderSize          = 8
)

// errBadFilterBlob is returned when a filter blob is truncated, the wrong
// version, or an unknown kind.
var errBadFilterBlob = errors.New("dedup: malformed seen-set filter blob")

// MarshalFilter serializes the resident filter to a portable blob (doc 10 section
// 6). It captures only the approximate tier; the exact set is rebuilt from the
// urlkey column on reload. The bytes are deterministic for a given filter state,
// so a checkpoint that did not change the filter writes the same region.
func (s *SeenSet) MarshalFilter() []byte {
	return s.filter.marshal()
}

func (f *filter) marshal() []byte {
	out := make([]byte, filterBlobHeaderSize, filterBlobHeaderSize+24+len(f.blocks)*8)
	out[0] = filterBlobVersion
	out[1] = filterKindBlockedBloom
	out[2] = uint8(f.k)
	// out[3] reserved, out[4:8] reserved, already zero.
	out = binary.LittleEndian.AppendUint64(out, f.nBlock)
	out = binary.LittleEndian.AppendUint64(out, f.n)
	out = binary.LittleEndian.AppendUint64(out, f.cap)
	for _, w := range f.blocks {
		out = binary.LittleEndian.AppendUint64(out, w)
	}
	return out
}

// residentMembership is the one-sided membership probe both filter forms answer:
// the mutable blocked-Bloom filter and the static ribbon snapshot. A reconstructed
// ResidentFilter holds whichever form the blob carried behind this interface, so
// the caller's MaybeContains is the same either way.
type residentMembership interface {
	maybeContains(key meguri.URLKey) bool
	bitsPerURL() float64
	length() uint64
}

// ResidentFilter is a reconstructed read-only seen-set filter: it answers the
// one-sided membership probe a discovery path uses to short-circuit "definitely
// not seen", and reports its resident cost, but it does not insert. A recovery
// loads it from the .meguri seen-set region and pairs it with the exact set
// rebuilt from the urlkey column. The blocked-Bloom and ribbon forms ride behind
// the same membership contract.
type ResidentFilter struct {
	mem residentMembership
}

// UnmarshalFilter reconstructs a resident filter from a filter blob, dispatching
// on the kind byte: the blocked-Bloom form MarshalFilter wrote, or the ribbon
// snapshot BuildRibbonFilter wrote. The reconstructed filter answers MaybeContains
// identically to the original for every key, the property the round-trip gate
// asserts.
func UnmarshalFilter(b []byte) (*ResidentFilter, error) {
	if len(b) < 2 || b[0] != filterBlobVersion {
		return nil, errBadFilterBlob
	}
	switch b[1] {
	case filterKindBlockedBloom:
		f, err := unmarshalBloom(b)
		if err != nil {
			return nil, err
		}
		return &ResidentFilter{mem: f}, nil
	case filterKindRibbon:
		rb, err := unmarshalRibbon(b)
		if err != nil {
			return nil, err
		}
		return &ResidentFilter{mem: rb}, nil
	case filterKindShardedRibbon:
		sr, err := unmarshalShardedRibbon(b)
		if err != nil {
			return nil, err
		}
		return &ResidentFilter{mem: sr}, nil
	default:
		return nil, errBadFilterBlob
	}
}

// unmarshalBloom reconstructs the blocked-Bloom filter from its kind-0 blob.
func unmarshalBloom(b []byte) (*filter, error) {
	if len(b) < filterBlobHeaderSize+24 {
		return nil, errBadFilterBlob
	}
	k := int(b[2])
	p := filterBlobHeaderSize
	nBlock := binary.LittleEndian.Uint64(b[p:])
	n := binary.LittleEndian.Uint64(b[p+8:])
	cap := binary.LittleEndian.Uint64(b[p+16:])
	body := b[p+24:]
	words := int(nBlock) * blockWords
	if len(body) != words*8 {
		return nil, errBadFilterBlob
	}
	blocks := make([]uint64, words)
	for i := range blocks {
		blocks[i] = binary.LittleEndian.Uint64(body[i*8:])
	}
	return &filter{blocks: blocks, nBlock: nBlock, k: k, n: n, cap: cap}, nil
}

// MaybeContains is the one-sided probe: false is authoritative (the key was
// never added), true is the filter's "probably", confirmed against the exact set
// by the caller.
func (r *ResidentFilter) MaybeContains(key meguri.URLKey) bool { return r.mem.maybeContains(key) }

// BitsPerURL reports the reconstructed filter's resident cost per held key.
func (r *ResidentFilter) BitsPerURL() float64 { return r.mem.bitsPerURL() }

// Len reports the number of keys the filter was built over.
func (r *ResidentFilter) Len() uint64 { return r.mem.length() }
