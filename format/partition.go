package format

import m "github.com/tamnd/meguri"

// Partition is the in-memory image of one .meguri file: the URL and host tables
// plus the shared string arena their *Ref fields point into. It is the unit a
// checkpoint writes and a load reads. The engine builds one from its live state
// and hands it to Encode; Decode hands one back on recovery or redistribution.
//
// The caller owns ordering: URLs must be sorted by URLKey big-endian and hosts
// by HostKey ascending. Encode verifies this rather than sorting, so a buggy
// caller is caught instead of silently reordering rows the column zone maps and
// the seen-set assume are sorted.
type Partition struct {
	ID           uint32
	HostKeyLo    uint64
	HostKeyHi    uint64
	CreatedHours uint32
	DefaultCodec uint8 // CodecNone or CodecZstd; M0 writes RAW pages either way

	URLs    []m.URLRecord
	Hosts   []m.HostRecord
	Strings []byte // arena the URLRef/ETagRef/HostRef/RegistrableRef offsets index

	// SeenFilter is the optional serialized resident seen-set filter (doc 10
	// section 6, the seen-set filter region). It is the approximate dedup tier
	// (dedup.SeenSet.MarshalFilter) carried across a checkpoint so a reload does
	// not re-add every key; empty means the region is omitted and the filter is
	// rebuilt from the urlkey column on load. The bytes are opaque to the format:
	// the dedup package owns their layout, the format frames them with a CRC.
	SeenFilter []byte

	Meta map[string]string // optional string metadata, keys sorted on write
}

// sortedURLs reports whether the URL rows are non-decreasing by URLKey.
func sortedURLs(recs []m.URLRecord) bool {
	for i := 1; i < len(recs); i++ {
		if recs[i].URLKey.Less(recs[i-1].URLKey) {
			return false
		}
	}
	return true
}

// sortedHosts reports whether the host rows are non-decreasing by HostKey.
func sortedHosts(recs []m.HostRecord) bool {
	for i := 1; i < len(recs); i++ {
		if recs[i].HostKey < recs[i-1].HostKey {
			return false
		}
	}
	return true
}
