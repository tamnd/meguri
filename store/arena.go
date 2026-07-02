package store

import "encoding/binary"

// The string arena is the live form of the .meguri string region (doc 10 section
// 7, doc 11 section 3.3): a flat byte buffer where every entry is a uvarint
// length followed by the bytes, and a record's *Ref field is a byte offset into
// it. Offset 0 is the "none" sentinel, so a fresh arena starts with a single
// zero byte and a zero reference reads back as the empty string. The format
// matches the frontier's arena exactly, so a checkpoint hands the arena straight
// to the file with no translation.

// appendUvarint appends v as an unsigned LEB128 varint, the fleet varint
// convention.
func appendUvarint(b []byte, v uint64) []byte {
	return binary.AppendUvarint(b, v)
}

// readArena reads the string interned at off. A zero or out-of-range offset, or
// a corrupt length, returns the empty string rather than panicking.
func readArena(arena []byte, off uint64) string {
	if off == 0 || off >= uint64(len(arena)) {
		return ""
	}
	n, k := binary.Uvarint(arena[off:])
	if k <= 0 {
		return ""
	}
	start := off + uint64(k)
	end := start + n
	if end > uint64(len(arena)) {
		return ""
	}
	return string(arena[start:end])
}

// readArenaBytes is readArena for a span the caller needs as raw bytes rather
// than a string, the form a packed robots blob is read in before it is unpacked.
// A zero or out-of-range offset, or a corrupt length, returns nil.
func readArenaBytes(arena []byte, off uint64) []byte {
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
