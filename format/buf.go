package format

import "encoding/binary"

// wbuf is a tiny append-only little-endian writer used to build the footer. It
// keeps the footer-building code readable without pulling in bytes.Buffer and
// its error returns that never fire on an in-memory grow.
type wbuf struct{ b []byte }

func (w *wbuf) u8(v uint8)       { w.b = append(w.b, v) }
func (w *wbuf) u32(v uint32)     { w.b = binary.LittleEndian.AppendUint32(w.b, v) }
func (w *wbuf) u64(v uint64)     { w.b = binary.LittleEndian.AppendUint64(w.b, v) }
func (w *wbuf) uvarint(v uint64) { w.b = binary.AppendUvarint(w.b, v) }
func (w *wbuf) bytes(p []byte)   { w.b = append(w.b, p...) }

// rbuf reads back what wbuf wrote. Every read is bounds-checked through the ok
// flag so a truncated or corrupt footer fails cleanly instead of panicking.
type rbuf struct {
	b   []byte
	pos int
	err error
}

func (r *rbuf) fail() bool { return r.err != nil }

func (r *rbuf) need(n int) bool {
	if r.err != nil {
		return false
	}
	if r.pos+n > len(r.b) {
		r.err = ErrCorrupt
		return false
	}
	return true
}

func (r *rbuf) u8() uint8 {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.pos]
	r.pos++
	return v
}

func (r *rbuf) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v
}

func (r *rbuf) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b[r.pos:])
	r.pos += 8
	return v
}

func (r *rbuf) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		r.err = ErrCorrupt
		return 0
	}
	r.pos += n
	return v
}

func (r *rbuf) bytes(n int) []byte {
	if !r.need(n) {
		return nil
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v
}
