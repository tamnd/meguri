package store

import (
	"encoding/binary"
	"io"
)

// spilledArena is the disk-backed string arena of spec 2072 doc 05 (D5): the
// canonical URL strings are not kept resident in a single []byte, they are read
// by byte offset from where they already live durably (the snapshot string
// region, or a log frame for strings written since the last checkpoint), through
// a bounded LRU (arenaCache, B_arena). This removes the old resident structure-3
// arena (~70 B/url, ~7 GiB at 100M) and replaces it with a flat B_arena budget.
//
// The accessor keeps the offset semantics of the resident readArena exactly: a
// record's *Ref is still a byte offset, offset 0 is still the none sentinel, the
// entry is still a uvarint length followed by the bytes. Only the byte source
// changes, from a resident slice to a positioned read against src. The decode is
// the same logic readArena runs (read the uvarint length, slice the bytes), so a
// spilled read is byte-identical to a resident read over the same bytes; that
// equality is the golden gate doc 05 section 7b requires.
//
// This is option B (pread + explicit LRU), the doc 05 section 2d default: the
// resident arena bytes are exactly the cache, bounded by B_arena and controlled
// by the engine, so the held-heap-vs-N slope check (doc 03) reads cleanly. The
// mmap alternative (option A) is the same interface with a different src whose
// ReadAt slices a mapped region; spilledArena does not care which it is handed.
type spilledArena struct {
	src   io.ReaderAt // durable string region, addressed by arena offset
	size  int64       // length of the region, the out-of-range bound
	cache *arenaCache
}

// arenaOverRead is the fixed span a cold read fetches in one positioned read. A
// canonical URL plus its uvarint length is almost always under this (doc 05
// section 2d: "most canonical URLs are < 100 bytes, so a single 256-byte pread
// usually gets the whole string and its length in one call"), so the common cold
// read is one syscall; a longer string takes a second exact read.
const arenaOverRead = 256

// newSpilledArena builds a spilled arena over src (the durable string region of
// length size) with the given resident byte budget. A budget of 0 means no
// resident cache: every read is a pread, the explicit floor of doc 05 section 3.
func newSpilledArena(src io.ReaderAt, size, budget int64) *spilledArena {
	return &spilledArena{src: src, size: size, cache: newArenaCache(budget)}
}

// readArenaAt resolves an arena offset to a string without a resident copy of the
// whole arena (doc 05 section 2c). The LRU is consulted first; on a miss the span
// is read from disk, decoded, and interned into the LRU. A zero or out-of-range
// offset, or a corrupt length, returns the empty string, matching readArena so
// the none sentinel and a stale reference both degrade to empty, never panic.
func (a *spilledArena) readArenaAt(off uint64) string {
	if off == 0 || off >= uint64(a.size) {
		return ""
	}
	if s, ok := a.cache.get(off); ok {
		return s // hot: working-set string, already resident
	}
	s, ok := a.readSpanFromDisk(off)
	if !ok {
		return ""
	}
	a.cache.put(off, s)
	return s
}

// readSpanFromDisk reads and decodes the uvarint-prefixed string at off with at
// most two positioned reads: a fixed over-read that usually captures the length
// and the whole string, then an exact read only when the string runs past the
// over-read. It returns ok=false on a short/empty region, a corrupt length, or a
// span that runs past the region end, the same degrade-to-empty cases readArena
// guards. The bytes are copied into a Go string (string(...)), so the read buffer
// is not retained and the cache holds only the immutable string.
func (a *spilledArena) readSpanFromDisk(off uint64) (string, bool) {
	head := make([]byte, arenaOverRead)
	n, err := a.src.ReadAt(head, int64(off))
	if n == 0 && err != nil && err != io.EOF {
		return "", false
	}
	head = head[:n]
	strLen, k := binary.Uvarint(head)
	if k <= 0 {
		return "", false
	}
	start := off + uint64(k)
	end := start + strLen
	if end > uint64(a.size) {
		return "", false
	}
	// The string fit inside the over-read: slice it out, no second syscall.
	if uint64(k)+strLen <= uint64(len(head)) {
		return string(head[uint64(k) : uint64(k)+strLen]), true
	}
	// Longer than the over-read: one exact positioned read for the full span.
	buf := make([]byte, strLen)
	m, err := a.src.ReadAt(buf, int64(start))
	if uint64(m) < strLen {
		if err != nil && err != io.EOF {
			return "", false
		}
		return "", false
	}
	return string(buf), true
}

// readArenaBytesAt is readArenaAt for a caller that needs the raw bytes (the
// packed robots blob path) rather than a string. It returns nil for the none
// sentinel or an out-of-range/corrupt offset, matching readArenaBytes. Robots
// blobs are O(hosts), not O(urls), so this does not need its own cache: it reuses
// the string read and copies out, which keeps the LRU keyed on one value per
// offset and never hands out a mutable view into a cached string.
func (a *spilledArena) readArenaBytesAt(off uint64) []byte {
	s := a.readArenaAt(off)
	if s == "" {
		return nil
	}
	return []byte(s)
}

// setSize updates the region bound after the store appends a freshly interned
// entry to the spill file. It is called under the store's arenaMu, the same lock
// that serializes every readArenaAt, so the bound a reader sees is always the
// length the writer just committed.
func (a *spilledArena) setSize(size int64) { a.size = size }

// stats exposes the cache accounting (resident bytes, hit rate) for the
// validation plan (doc 10) and the scale harness.
func (a *spilledArena) stats() (used, budget int64, hits, misses, evicted uint64) {
	return a.cache.stats()
}
