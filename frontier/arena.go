package frontier

import "encoding/binary"

// arena is the frontier's string store: a flat byte buffer where every entry is
// a uvarint length followed by the bytes. The *Ref fields on a URL or host
// record are byte offsets into it, exactly as the .meguri string region works
// (doc 10), so a checkpoint hands the arena straight to the file with no
// translation and a recovery reads it back the same way.
//
// Offset 0 is reserved as the "none" sentinel: a fresh arena starts with a
// single zero byte, so the first real string lands at offset >= 1 and a zero
// ETagRef or RedirectRef reads back as the empty string.
type arena struct{ buf []byte }

func newArena() arena { return arena{buf: []byte{0}} }

// intern appends s and returns its offset. Equal strings are not folded: the
// frontier interns each URL once at discovery, so the duplication a dictionary
// would remove is already absent, and the M7 codec cascade compresses the arena
// when it is written cold.
func (a *arena) intern(s string) uint64 {
	off := uint64(len(a.buf))
	a.buf = binary.AppendUvarint(a.buf, uint64(len(s)))
	a.buf = append(a.buf, s...)
	return off
}

// str reads back the string interned at off. A zero or out-of-range offset
// returns the empty string, so the none sentinel and a corrupt reference both
// degrade to empty rather than panicking.
func (a *arena) str(off uint64) string {
	if off == 0 || off >= uint64(len(a.buf)) {
		return ""
	}
	n, k := binary.Uvarint(a.buf[off:])
	if k <= 0 {
		return ""
	}
	start := off + uint64(k)
	end := start + n
	if end > uint64(len(a.buf)) {
		return ""
	}
	return string(a.buf[start:end])
}
