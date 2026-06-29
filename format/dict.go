package format

// dict.go holds the two doc 10 section 7 refinements that sit on top of the flat
// string arena: a content-deduplicating interner so a span that repeats across
// rows is stored once, and a robots blob packing that picks the smallest of the
// allow-all sentinel, raw, or compressed for each blob.
//
// The arena (blob.go) keys nothing: arenaIntern appends every span, so the same
// registrable domain interned for www.example.com and shop.example.com lands
// twice, and an identical allow-all robots blob lands once per host. The shared
// dictionary closes that: it keys spans by content, so a repeat returns the
// offset the first copy already holds. The compactor (ops.go) builds its rebased
// arena through one of these, so a split or merge folds duplicate host,
// registrable, ETag, and robots spans down to a single copy across both inputs.

// arenaDict is a string arena that interns each distinct span once. It wraps the
// flat arena with a content index, so intern returns the existing offset when the
// same bytes are interned again. Offset 0 stays the none sentinel.
type arenaDict struct {
	arena []byte
	by    map[string]uint64
}

// newDict returns an empty dictionary holding only the none sentinel at offset 0.
func newDict() *arenaDict {
	return &arenaDict{arena: newArena(), by: map[string]uint64{}}
}

// intern returns the offset of span in the arena, appending it only if the same
// bytes are not already interned. Equal content always maps to one offset, which
// is what folds repeated host and registrable strings down to a single copy.
func (d *arenaDict) intern(span []byte) uint64 {
	if off, ok := d.by[string(span)]; ok {
		return off
	}
	var off uint64
	d.arena, off = arenaIntern(d.arena, span)
	d.by[string(span)] = off
	return off
}

// Robots blob packing (doc 10 section 7). A robots blob is the parsed-rules form
// a host's RobotsRef points at. Most hosts serve an allow-all policy (no file, an
// empty file, or a group with no disallow), so the common blob is empty; the rest
// are short text that compresses well in bulk but not always alone. The packing
// gives each blob a one-byte mode and stores the smallest of three forms, so the
// arena pays one byte for the overwhelmingly common allow-all case and never
// pays for compression that does not help a given blob.
const (
	robotsAllowAll   = 0 // empty body, the allow-all sentinel
	robotsRaw        = 1 // body is the blob verbatim
	robotsCompressed = 2 // body is the blob run through the block codec
)

// PackRobots wraps a robots blob in the smallest of the three modes: the
// allow-all sentinel for an empty blob, then whichever of raw or codec-compressed
// is shorter. The first byte is always the mode, so UnpackRobots needs nothing
// else to reverse it. An empty blob packs to a single sentinel byte.
func PackRobots(blob []byte, codec uint8) []byte {
	if len(blob) == 0 {
		return []byte{robotsAllowAll}
	}
	used, comp := compress(codec, blob)
	if used != CodecNone && len(comp) < len(blob) {
		out := make([]byte, 0, len(comp)+1)
		out = append(out, robotsCompressed)
		return append(out, comp...)
	}
	out := make([]byte, 0, len(blob)+1)
	out = append(out, robotsRaw)
	return append(out, blob...)
}

// UnpackRobots reverses PackRobots, returning the original blob and whether the
// bytes were a well-formed packed blob. A compressed body is bounded by sizeHint
// so a corrupt length cannot drive an unbounded allocation; pass the codec the
// blob was packed with. Empty input or an unknown mode reports not-ok.
func UnpackRobots(packed []byte, codec uint8, sizeHint int) ([]byte, bool) {
	if len(packed) == 0 {
		return nil, false
	}
	body := packed[1:]
	switch packed[0] {
	case robotsAllowAll:
		if len(body) != 0 {
			return nil, false
		}
		return nil, true
	case robotsRaw:
		return body, true
	case robotsCompressed:
		out, err := decompress(codec, body, sizeHint)
		if err != nil {
			return nil, false
		}
		return out, true
	default:
		return nil, false
	}
}

// RobotsSizeHint bounds an unpacked robots blob. A robots.txt that matters for
// crawl scheduling is small; a blob claiming to inflate past this is treated as
// corrupt rather than allocated.
const RobotsSizeHint = 1 << 20
