package format

import (
	"io"

	m "github.com/tamnd/meguri"
)

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

	// StringsAt and StringsSize are the streaming alternative to Strings for the
	// bounded-memory checkpoint (StreamEncodeToFile). When StringsAt is non-nil the
	// encoder reads the [0, StringsSize) string arena from it through a bounded
	// chunk buffer and writes the blob region page by page, so a 100M checkpoint
	// never materializes the whole multi-gigabyte arena in RAM. The streamed pages
	// decode to the same bytes Strings would have framed as one page, so the *Ref
	// offsets are identical. A non-nil StringsAt takes precedence over Strings.
	StringsAt   io.ReaderAt
	StringsSize int64

	// SeenFilter is the optional serialized resident seen-set filter (doc 10
	// section 6, the seen-set filter region). It is the approximate dedup tier
	// (dedup.SeenSet.MarshalFilter) carried across a checkpoint so a reload does
	// not re-add every key; empty means the region is omitted and the filter is
	// rebuilt from the urlkey column on load. The bytes are opaque to the format:
	// the dedup package owns their layout, the format frames them with a CRC.
	SeenFilter []byte

	// BuildSchedule asks Encode to derive and write the schedule index region (doc
	// 10 section 7, the bucketed timing wheel) from the URL rows' next_due. It is
	// off by default so a partition that does not want the wheel stays byte-for-byte
	// the same; a checkpoint that wants a scheduler to find due work without
	// scanning the next_due column sets it. The region is omitted anyway when no row
	// is scheduled.
	BuildSchedule bool

	// MaxPageRows caps how many rows one column page holds. Zero (the default) keeps
	// the M0 behavior of one page per column, so a partition that does not opt in
	// stays byte-for-byte the same and the pinned size baselines do not move. A
	// positive value spills a column past that many rows into successive pages and
	// builds a per-page skip list (doc 10 section 4, the page_index_offset and inline
	// page min/max): each page carries its own zone min/max so a reader prunes at the
	// page level, decompressing only the pages whose range overlaps a predicate
	// rather than the whole column. doc 14 sets the production page size; this is the
	// mechanism it turns on.
	MaxPageRows int

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
