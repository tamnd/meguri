package format

import (
	"encoding/binary"
	"errors"
)

// ErrArenaBackward is returned by ArenaSeqReader.At when a ref moves backward, which
// breaks the ascending-access contract the sequential reader relies on.
var ErrArenaBackward = errors.New("format: arena ref moved backward")

// ArenaSeqReader resolves string-arena refs in ascending order without holding the
// whole (possibly multi-gigabyte) arena resident. It is the read side of Stage 2
// compaction (spec 2073 doc 08): the compactor walks the base URL table in URLKey
// order to source each unchanged record's URL string, and BulkLoad interns URL
// strings in that same key order, so a record's URLRef only ever increases as the
// cursor advances. This reader decodes the blob region's pages in order and keeps a
// sliding window of decoded bytes starting at the last resolved ref, so its transient
// is about one blob page plus the current span, not the arena. The whole-arena
// decodeBlobRegion is the wrong tool at 100M scale; this is the bounded one.
//
// Host strings live at the low end of the arena (BulkLoad interns hosts before URLs),
// so a compactor resolves the host table's refs through one reader first, then opens a
// second reader for the ascending URL walk. Two forward passes, never a random seek.
type ArenaSeqReader struct {
	region []byte // the file's blob region, decoded a page at a time
	off    int    // next page byte offset within region
	base   uint64 // arena offset of window[0]
	window []byte // decoded arena bytes from base forward
	end    uint64 // arena offset one past the last decoded byte
	full   bool   // every page consumed
}

// ArenaSeqReader opens a sequential reader over the file's string-blob region. A file
// with no blob region yields a reader that resolves every ref to nil, matching an
// empty arena.
func (r *Reader) ArenaSeqReader() *ArenaSeqReader {
	reg, ok := findRegion(r.footer.regions, RegionStringBlob)
	if !ok {
		return &ArenaSeqReader{full: true}
	}
	return &ArenaSeqReader{region: r.file[reg.offset : reg.offset+reg.length]}
}

// decodeNext appends the next blob page's decoded payload to the window and advances
// end. It returns false when the region is exhausted.
func (a *ArenaSeqReader) decodeNext() (bool, error) {
	if a.off >= len(a.region) {
		a.full = true
		return false, nil
	}
	_, payload, consumed, err := readPage(a.region[a.off:])
	if err != nil {
		return false, err
	}
	a.off += consumed
	a.window = append(a.window, payload...)
	a.end += uint64(len(payload))
	return true, nil
}

// ensure decodes pages until the window covers arena offset target (exclusive) or the
// region is exhausted. It returns whether the target is covered.
func (a *ArenaSeqReader) ensure(target uint64) (bool, error) {
	for a.end < target {
		ok, err := a.decodeNext()
		if err != nil {
			return false, err
		}
		if !ok {
			return a.end >= target, nil
		}
	}
	return true, nil
}

// advance drops the window prefix below ref so the window starts at ref, decoding
// forward first if ref is past what has been read. It bounds the resident window to
// about one page plus the pending span.
func (a *ArenaSeqReader) advance(ref uint64) error {
	if ref < a.base {
		return ErrArenaBackward
	}
	if _, err := a.ensure(ref + 1); err != nil {
		return err
	}
	if ref > a.base {
		drop := ref - a.base
		if drop >= uint64(len(a.window)) {
			// ref is at or past the arena end; leave an empty window at ref so a
			// past-end read below reports the corruption rather than panicking.
			a.window = a.window[len(a.window):]
			a.base = a.end
		} else {
			a.window = a.window[drop:]
			a.base = ref
		}
	}
	return nil
}

// At returns the string interned at ref, valid until the next At call. ref must be at
// or after the previous call's ref (ascending access); a zero ref is the absent
// sentinel and returns nil. A backward ref returns ErrArenaBackward; a ref or span
// past the arena returns ErrCorrupt.
func (a *ArenaSeqReader) At(ref uint64) ([]byte, error) {
	if ref == 0 {
		return nil, nil
	}
	if err := a.advance(ref); err != nil {
		return nil, err
	}
	// A ref at or past the fully decoded arena end is absent, matching arenaRead's
	// tolerance rather than a hard corruption error.
	if a.full && a.base >= a.end {
		return nil, nil
	}
	// The length prefix is a uvarint of at most binary.MaxVarintLen64 bytes; make sure
	// that many (or the arena's remainder) are decoded before reading it.
	if _, err := a.ensure(a.base + binary.MaxVarintLen64); err != nil {
		return nil, err
	}
	n, k := binary.Uvarint(a.window)
	if k <= 0 {
		return nil, ErrCorrupt
	}
	spanEnd := a.base + uint64(k) + n
	covered, err := a.ensure(spanEnd)
	if err != nil {
		return nil, err
	}
	if !covered {
		return nil, ErrCorrupt
	}
	return a.window[k : uint64(k)+n], nil
}
