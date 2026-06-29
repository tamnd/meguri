package format

import (
	"encoding/binary"
	"math"
	"sort"
)

// The collection manifest (doc 10 section 11) is the catalog that binds the
// per-partition .meguri files into one fleet frontier, the way tatami.manifest
// binds document shards. One .meguri file is one partition; the manifest is the
// map of all partitions, the thing a router or a fleet-wide query reads to find
// which file holds a given host without opening any file.
//
// It serves three fleet operations: routing a HostKey to its owning partition
// by a binary search over the sorted ranges, fleet-wide scheduling by reading
// only the per-partition due_min (a zone map over partitions, so a control plane
// skips a partition with no due work fleet-wide), and rebalancing by reading the
// ranges and sizes without opening every file.

// ManifestMagic brackets a serialized manifest. It is distinct from the file
// magic so a manifest is never mistaken for a partition file.
var ManifestMagic = [4]byte{'M', 'G', 'M', '1'}

// ManifestEntry is one partition's row in the catalog, the durable record of
// its range, size, and soonest due time pulled from the file's STATS.
type ManifestEntry struct {
	PartitionID  uint32
	FileRef      string // path or object-store key of the .meguri file
	HostKeyLo    uint64
	HostKeyHi    uint64
	URLCount     uint64
	HostCount    uint64
	DueMin       uint32
	BytesPerURL  float32
	FileCRC32C   uint32 // a CRC over the file's footer, a cheap identity check
	CreatedHours uint32
	Epoch        uint32
}

// Manifest is the parsed catalog: the partition entries sorted by HostKeyLo so
// a route is a binary search.
type Manifest struct {
	Entries []ManifestEntry
}

// ManifestEntryFor builds a manifest entry from a freshly written file's bytes
// and its storage ref, reading the header and footer once. The epoch tags which
// partition-map generation the entry belongs to (doc 10 section 11, D14).
func ManifestEntryFor(fileBytes []byte, fileRef string, epoch uint32) (ManifestEntry, error) {
	ins, err := InspectBytes(fileBytes)
	if err != nil {
		return ManifestEntry{}, err
	}
	footerCRC := crc32c(footerSpan(fileBytes))
	return ManifestEntry{
		PartitionID:  ins.PartitionID,
		FileRef:      fileRef,
		HostKeyLo:    ins.HostKeyLo,
		HostKeyHi:    ins.HostKeyHi,
		URLCount:     ins.URLCount,
		HostCount:    ins.HostCount,
		DueMin:       ins.Stats.DueMin,
		BytesPerURL:  ins.Stats.BytesPerURL,
		FileCRC32C:   footerCRC,
		CreatedHours: ins.CreatedHours,
		Epoch:        epoch,
	}, nil
}

// footerSpan returns the on-disk footer bytes of a file, the scope the manifest
// CRC covers (doc 10 section 8, the file_crc32c reuse of the footer scope).
func footerSpan(b []byte) []byte {
	if len(b) < trailerSize {
		return nil
	}
	r := &rbuf{b: b[len(b)-trailerSize:]}
	footerLen := int(r.u32())
	start := len(b) - trailerSize - footerLen
	if start < 0 {
		return nil
	}
	return b[start : len(b)-trailerSize]
}

// BuildManifest assembles a manifest from entries, sorting by HostKeyLo so the
// route lookup is a binary search and the ranges read in order.
func BuildManifest(entries []ManifestEntry) *Manifest {
	out := append([]ManifestEntry(nil), entries...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].HostKeyLo != out[j].HostKeyLo {
			return out[i].HostKeyLo < out[j].HostKeyLo
		}
		return out[i].PartitionID < out[j].PartitionID
	})
	return &Manifest{Entries: out}
}

// Route returns the partition entry whose [HostKeyLo, HostKeyHi] contains hk,
// found by binary search over the sorted ranges. ok is false when no partition
// owns hk, which a caller treats as a gap in the range coverage.
func (m *Manifest) Route(hk uint64) (ManifestEntry, bool) {
	// Find the last entry whose HostKeyLo <= hk, then check it contains hk.
	i := sort.Search(len(m.Entries), func(i int) bool { return m.Entries[i].HostKeyLo > hk })
	if i == 0 {
		return ManifestEntry{}, false
	}
	e := m.Entries[i-1]
	if hk >= e.HostKeyLo && hk <= e.HostKeyHi {
		return e, true
	}
	return ManifestEntry{}, false
}

// DueParts returns the partitions with due work at or before now: those whose
// DueMin is nonzero and not in the future. This is the fleet-level pushdown of
// doc 10 section 9 lifted to partitions, reading only the manifest.
func (m *Manifest) DueParts(now uint32) []ManifestEntry {
	var out []ManifestEntry
	for _, e := range m.Entries {
		if e.DueMin != 0 && e.DueMin <= now {
			out = append(out, e)
		}
	}
	return out
}

// CoverageGap returns the first HostKey range gap or overlap in the manifest
// within one epoch, or ok=false when the entries tile [0, 2^64) cleanly. The
// ranges must partition the space with no gap and no overlap (doc 10 section 11,
// D14), and this is the check a control plane runs before trusting the map.
func (m *Manifest) CoverageGap(epoch uint32) (lo, hi uint64, ok bool) {
	var next uint64
	first := true
	for _, e := range m.Entries {
		if e.Epoch != epoch {
			continue
		}
		if first {
			if e.HostKeyLo != 0 {
				return 0, e.HostKeyLo - 1, true // gap before the first entry
			}
			first = false
		} else if e.HostKeyLo != next {
			if e.HostKeyLo > next {
				return next, e.HostKeyLo - 1, true // gap
			}
			return e.HostKeyLo, next - 1, true // overlap
		}
		next = e.HostKeyHi + 1
	}
	if first {
		return 0, ^uint64(0), true // no entries cover the space at all
	}
	if next != 0 { // next wrapped to 0 means the last entry reached 2^64-1
		return next, ^uint64(0), true
	}
	return 0, 0, false
}

// EncodeManifest serializes a manifest deterministically: the magic, the entry
// count, then each entry, then the trailing CRC and magic, mirroring the file's
// commit discipline so a torn manifest is detectable.
func EncodeManifest(m *Manifest) []byte {
	var w wbuf
	w.bytes(ManifestMagic[:])
	w.uvarint(uint64(len(m.Entries)))
	for _, e := range m.Entries {
		w.u32(e.PartitionID)
		w.uvarint(uint64(len(e.FileRef)))
		w.bytes([]byte(e.FileRef))
		w.u64(e.HostKeyLo)
		w.u64(e.HostKeyHi)
		w.u64(e.URLCount)
		w.u64(e.HostCount)
		w.u32(e.DueMin)
		w.b = appF32(w.b, e.BytesPerURL)
		w.u32(e.FileCRC32C)
		w.u32(e.CreatedHours)
		w.u32(e.Epoch)
	}
	body := w.b
	var tail wbuf
	tail.u32(crc32c(body))
	tail.bytes(ManifestMagic[:])
	return append(body, tail.b...)
}

// DecodeManifest parses what EncodeManifest wrote, verifying the magic at both
// ends and the body CRC. The entries are returned in their written (sorted)
// order, so Route works without a re-sort.
func DecodeManifest(b []byte) (*Manifest, error) {
	if len(b) < 4+8 || [4]byte(b[:4]) != ManifestMagic {
		return nil, ErrBadMagic
	}
	if [4]byte(b[len(b)-4:]) != ManifestMagic {
		return nil, ErrBadMagic
	}
	body := b[:len(b)-8]
	if crc32c(body) != binary.LittleEndian.Uint32(b[len(b)-8:]) {
		return nil, ErrChecksum
	}
	r := &rbuf{b: body, pos: 4}
	n := int(r.uvarint())
	if r.fail() {
		return nil, ErrCorrupt
	}
	entries := make([]ManifestEntry, 0, n)
	for range n {
		e := ManifestEntry{}
		e.PartitionID = r.u32()
		refLen := int(r.uvarint())
		e.FileRef = string(r.bytes(refLen))
		e.HostKeyLo = r.u64()
		e.HostKeyHi = r.u64()
		e.URLCount = r.u64()
		e.HostCount = r.u64()
		e.DueMin = r.u32()
		e.BytesPerURL = math.Float32frombits(r.u32())
		e.FileCRC32C = r.u32()
		e.CreatedHours = r.u32()
		e.Epoch = r.u32()
		entries = append(entries, e)
	}
	if r.fail() {
		return nil, ErrCorrupt
	}
	return &Manifest{Entries: entries}, nil
}
