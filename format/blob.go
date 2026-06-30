package format

import (
	"encoding/binary"
	"io"
)

// The string and blob region (doc 10 section 7) is the flat byte arena the
// *Ref columns point into: the canonical URLs, the host and registrable-domain
// strings, the ETags, and the robots blobs. M0 stored it verbatim, which left
// it the single largest part of the file (on the ccrawl slice 54.6 of the 77.76
// bytes per url were raw URL strings). M7 frames the region as one blob page so
// the block codec runs over it, taking the URL strings from 54.6 down toward the
// 13.5 bytes per url zstd reaches on the sorted, host-clustered arena.
//
// The references the columns carry are byte offsets into the *uncompressed*
// arena, so they are unchanged by this: a reader decodes the region once to
// recover the arena, then resolves a ref exactly as before. Per-ref random
// access without decoding the whole arena, the FSST-span layout doc 10 section
// 7 describes, is the documented refinement on top of this; the hot read paths
// (the scheduler scan, the dedup check) never resolve a URL string, so whole
// arena framing serves M7's checkpoint, recovery, and redistribution roles.

// encodeBlobRegion frames the string arena as one blob page under the given
// block codec. An empty arena yields no region, matching M0.
func encodeBlobRegion(arena []byte, codec uint8) []byte {
	if len(arena) == 0 {
		return nil
	}
	return writePage(PageBlob, EncRaw, codec, uint32(len(arena)), 0, 0, arena)
}

// decodeBlobRegion recovers the string arena from a blob region's bytes,
// verifying each page CRC and decompressing through its recorded codec.
//
// The materializing encode (encodeBlobRegion) writes the arena as one page, so
// the common path is a single readPage. The streaming encode
// (streamBlobRegion) writes the arena as a run of fixed-size pages concatenated
// in order, so a 100M checkpoint never holds the whole multi-gigabyte arena in
// RAM at once. Concatenating the pages' decoded payloads in order rebuilds the
// exact uncompressed arena bytes either way, so the *Ref offsets the columns
// carry resolve identically regardless of how many pages the region holds.
func decodeBlobRegion(region []byte) ([]byte, error) {
	if len(region) == 0 {
		return nil, nil
	}
	var arena []byte
	for off := 0; off < len(region); {
		_, payload, consumed, err := readPage(region[off:])
		if err != nil {
			return nil, err
		}
		arena = append(arena, payload...)
		off += consumed
	}
	return arena, nil
}

// streamBlobRegion writes the string arena as a run of fixed-size blob pages,
// each independently codec-framed, reading the arena from src through a bounded
// chunk buffer so the encode's transient is one chunk, not the whole arena. The
// decoded pages concatenate to the same bytes encodeBlobRegion would have
// written as one page, so *Ref offsets are unchanged. It returns the number of
// region bytes written. A zero size yields no region, matching M0.
func streamBlobRegion(w io.Writer, src io.ReaderAt, size int64, chunk int, codec uint8) (int64, error) {
	if size <= 0 {
		return 0, nil
	}
	if chunk <= 0 {
		chunk = blobStreamChunk
	}
	buf := make([]byte, chunk)
	var written int64
	for base := int64(0); base < size; {
		n := chunk
		if rem := size - base; rem < int64(n) {
			n = int(rem)
		}
		if _, err := src.ReadAt(buf[:n], base); err != nil {
			return written, err
		}
		page := writePage(PageBlob, EncRaw, codec, uint32(n), 0, uint64(base), buf[:n])
		if _, err := w.Write(page); err != nil {
			return written, err
		}
		written += int64(len(page))
		base += int64(n)
	}
	return written, nil
}

// blobStreamChunk is the default uncompressed bytes per streamed blob page. It
// bounds the streaming encode's transient (one chunk read plus its compressed
// output) and is large enough that the per-page 32-byte header is negligible
// against the arena: at 8 MiB a 6.5 GiB arena is about 830 pages, 26 KiB of
// headers.
const blobStreamChunk = 8 << 20

// newArena returns a fresh string arena holding only the none sentinel: a
// single zero byte at offset 0, so a zero *Ref reads back as absent (doc 10
// section 7, doc 11 section 3.3). Every interned span lives at a positive
// offset.
func newArena() []byte { return []byte{0} }

// arenaIntern appends s to the arena as a uvarint length followed by the bytes,
// the fleet arena format the store uses (store/arena.go), and returns the
// offset the span starts at. An empty string interns to a real offset too, so a
// caller that wants the none sentinel passes a zero ref rather than an empty
// string.
func arenaIntern(arena []byte, s []byte) ([]byte, uint64) {
	off := uint64(len(arena))
	arena = binary.AppendUvarint(arena, uint64(len(s)))
	arena = append(arena, s...)
	return arena, off
}

// arenaRead returns the span interned at off, or nil for the zero sentinel or an
// out-of-range or corrupt offset, matching store/arena.go's tolerance.
func arenaRead(arena []byte, off uint64) []byte {
	if off == 0 || off >= uint64(len(arena)) {
		return nil
	}
	n, k := binary.Uvarint(arena[off:])
	if k <= 0 {
		return nil
	}
	start := off + uint64(k)
	end := start + n
	if end > uint64(len(arena)) {
		return nil
	}
	return arena[start:end]
}
