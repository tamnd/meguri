package format

import (
	"bufio"
	"encoding/binary"
	"io"
)

// The string and blob region (doc 10 section 7) is the flat byte arena the
// *Ref columns point into: the canonical URLs, the host and registrable-domain
// strings, the ETags, and the robots blobs. M0 stored it verbatim, which left
// it the single largest part of the file (on the ccrawl slice 54.6 of the 77.76
// bytes per url were raw URL strings). The region is framed as blob pages so the
// block codec runs over it, taking the URL strings toward the ~13.5 bytes per url
// zstd reaches on the sorted, host-clustered arena.
//
// Spec 2074 M1 adds front-coding ahead of the codec. The arena's spans are
// URLKey-ordered and host-clustered, so adjacent strings share long prefixes;
// front-coding stores each string as the shared-prefix length with the previous
// string in its page plus the literal suffix, making the sharing explicit so zstd
// spends nothing rediscovering it (the bake-off in Spec 2074 doc 01 measured ~7%
// off the region). The first string of every page is a restart (shared 0), so a
// page reverses on its own, and decode reconstructs the exact raw arena bytes.
//
// The references the columns carry are byte offsets into the *uncompressed* raw
// arena, so front-coding does not change them: a reader decodes the region back to
// the raw arena, then resolves a ref exactly as before. The hot read paths (the
// scheduler scan, the dedup check) never resolve a URL string, so whole-arena
// framing serves the checkpoint, recovery, and redistribution roles, and the
// front-coding is invisible above the page decode.

// encodeBlobRegion frames the string arena as one blob page under the given block
// codec. When frontCode is set the page payload is front-coded (EncFrontCode)
// ahead of the codec; otherwise the raw arena bytes are framed (EncRaw). An empty
// arena yields no region, matching M0.
func encodeBlobRegion(arena []byte, codec uint8, frontCode bool) []byte {
	if len(arena) == 0 {
		return nil
	}
	if frontCode {
		spans, lead := parseArenaSpans(arena)
		payload := frontCodePayload(spans, lead)
		return writePage(PageBlob, EncFrontCode, codec, uint32(len(arena)), 0, 0, payload)
	}
	return writePage(PageBlob, EncRaw, codec, uint32(len(arena)), 0, 0, arena)
}

// decodeBlobRegion recovers the string arena from a blob region's bytes, verifying
// each page CRC and decompressing through its recorded codec. A front-coded page
// (EncFrontCode) is reversed back to the raw arena bytes; a raw page is taken as
// is. Concatenating the pages' raw payloads in order rebuilds the exact arena
// either way, so the *Ref offsets the columns carry resolve identically regardless
// of the layout or how many pages the region holds.
func decodeBlobRegion(region []byte) ([]byte, error) {
	if len(region) == 0 {
		return nil, nil
	}
	var arena []byte
	for off := 0; off < len(region); {
		h, payload, consumed, err := readPage(region[off:])
		if err != nil {
			return nil, err
		}
		if h.encoding == EncFrontCode {
			raw, err := unFrontCodePayload(payload)
			if err != nil {
				return nil, err
			}
			arena = append(arena, raw...)
		} else {
			arena = append(arena, payload...)
		}
		off += consumed
	}
	return arena, nil
}

// streamBlobRegion writes the string arena as a run of blob pages, reading the
// arena from src so the encode's transient is one page, not the whole arena. The
// decoded pages concatenate to the same raw bytes encodeBlobRegion would have
// written, so *Ref offsets are unchanged. When frontCode is set each page is
// front-coded with a per-page restart and bounded by chunk raw bytes; otherwise
// the arena is framed in fixed chunk-sized raw pages. It returns the number of
// region bytes written. A zero size yields no region, matching M0.
func streamBlobRegion(w io.Writer, src io.ReaderAt, size int64, chunk int, codec uint8, frontCode bool) (int64, error) {
	if size <= 0 {
		return 0, nil
	}
	if chunk <= 0 {
		chunk = blobStreamChunk
	}
	if frontCode {
		return streamFrontCodedBlob(w, src, size, chunk, codec)
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

// streamFrontCodedBlob streams the raw arena from src as front-coded blob pages.
// It parses the arena's leading sentinel and its uvarint-length spans in order,
// front-coding each string against the previous string in the current page and
// flushing a page once its reconstructed raw size reaches chunk. The first page
// carries the sentinel; each page restarts the prefix so it decodes on its own.
func streamFrontCodedBlob(w io.Writer, src io.ReaderAt, size int64, chunk int, codec uint8) (int64, error) {
	br := bufio.NewReaderSize(io.NewSectionReader(src, 0, size), 1<<20)
	// The arena always opens with the one-byte absent sentinel (blob.go newArena);
	// it becomes the first page's lead marker and every real span follows it.
	sentinel, err := br.ReadByte()
	if err != nil {
		return 0, err
	}

	var written int64
	var prev []byte
	payload := make([]byte, 0, chunk+chunk/8)
	// Page 0 carries the sentinel; later pages do not. rawInPage tracks the raw
	// arena bytes the page reconstructs, so a page holds about chunk raw bytes.
	rawInPage := 0
	hasSpan := false
	startPage := func(withSentinel bool) {
		payload = payload[:0]
		rawInPage = 0
		hasSpan = false
		prev = prev[:0]
		if withSentinel {
			payload = append(payload, 1)
			rawInPage = 1
		} else {
			payload = append(payload, 0)
		}
	}
	flush := func() error {
		page := writePage(PageBlob, EncFrontCode, codec, uint32(rawInPage), 0, 0, payload)
		if _, err := w.Write(page); err != nil {
			return err
		}
		written += int64(len(page))
		return nil
	}

	startPage(sentinel == 0)
	for {
		n, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return written, err
		}
		s := make([]byte, n)
		if _, err := io.ReadFull(br, s); err != nil {
			return written, err
		}
		if hasSpan && rawInPage >= chunk {
			if err := flush(); err != nil {
				return written, err
			}
			startPage(false)
		}
		shared := commonPrefixBytes(prev, s)
		payload = binary.AppendUvarint(payload, uint64(shared))
		payload = binary.AppendUvarint(payload, uint64(len(s)-shared))
		payload = append(payload, s[shared:]...)
		rawInPage += uvarintLen(uint64(len(s))) + len(s)
		hasSpan = true
		prev = append(prev[:0], s...)
	}
	// Always flush the trailing page: it holds the last spans, or on an arena of
	// nothing but the sentinel it holds just the lead marker so decode yields [0].
	if err := flush(); err != nil {
		return written, err
	}
	return written, nil
}

// frontCodePayload builds a blob-page payload from a run of arena spans: a leading
// byte marking whether the reconstructed raw bytes begin with the arena sentinel,
// then one entry per string as uvarint(shared), uvarint(len-shared), suffix. prev
// is empty at the start, so the first entry is a restart (shared 0) and the page
// reverses without any earlier page.
func frontCodePayload(spans [][]byte, leadSentinel bool) []byte {
	out := make([]byte, 0, 1+len(spans)*8)
	if leadSentinel {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	var prev []byte
	for _, s := range spans {
		shared := commonPrefixBytes(prev, s)
		out = binary.AppendUvarint(out, uint64(shared))
		out = binary.AppendUvarint(out, uint64(len(s)-shared))
		out = append(out, s[shared:]...)
		prev = s
	}
	return out
}

// unFrontCodePayload reverses frontCodePayload back to the exact raw arena bytes:
// the leading sentinel (if any) followed by each span re-emitted as its uvarint
// length and bytes. A shared length past the previous string, a truncated suffix,
// or a bad marker byte is corruption.
func unFrontCodePayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, ErrCorrupt
	}
	var raw []byte
	switch payload[0] {
	case 1:
		raw = append(raw, 0)
	case 0:
	default:
		return nil, ErrCorrupt
	}
	pos := 1
	var prev []byte
	for pos < len(payload) {
		shared, k1 := binary.Uvarint(payload[pos:])
		if k1 <= 0 {
			return nil, ErrCorrupt
		}
		pos += k1
		sufLen, k2 := binary.Uvarint(payload[pos:])
		if k2 <= 0 {
			return nil, ErrCorrupt
		}
		pos += k2
		end := pos + int(sufLen)
		if end < pos || end > len(payload) {
			return nil, ErrCorrupt
		}
		if shared > uint64(len(prev)) {
			return nil, ErrCorrupt
		}
		full := make([]byte, int(shared)+int(sufLen))
		copy(full, prev[:shared])
		copy(full[shared:], payload[pos:end])
		pos = end
		raw = binary.AppendUvarint(raw, uint64(len(full)))
		raw = append(raw, full...)
		prev = full
	}
	return raw, nil
}

// parseArenaSpans splits a whole raw arena into its interned spans, returning the
// span byte slices and whether the arena opens with the absent sentinel. Offset 0
// is the reserved sentinel (blob.go newArena), so real spans are parsed from
// offset 1 as uvarint-length-prefixed runs.
func parseArenaSpans(arena []byte) ([][]byte, bool) {
	if len(arena) == 0 {
		return nil, false
	}
	lead := arena[0] == 0
	pos := 0
	if lead {
		pos = 1
	}
	var spans [][]byte
	for pos < len(arena) {
		n, k := binary.Uvarint(arena[pos:])
		if k <= 0 {
			break
		}
		start := pos + k
		end := start + int(n)
		if end > len(arena) {
			break
		}
		spans = append(spans, arena[start:end])
		pos = end
	}
	return spans, lead
}

// commonPrefixBytes returns the length of the shared leading bytes of a and b.
func commonPrefixBytes(a, b []byte) int {
	n := min(len(b), len(a))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// uvarintLen returns the number of bytes binary.PutUvarint uses for v, the raw
// arena size a front-coded span reconstructs to (its length prefix plus bytes).
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
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
