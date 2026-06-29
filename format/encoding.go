package format

import "encoding/binary"

// This file is the tatami codec cascade (doc 10 section 3): the per-column value
// encodings that turn a fixed-width column-major page into a small one. M0 wrote
// every column EncRaw; M7 adds RLE, DICTIONARY, DELTA, FOR, and DELTA_FOR, and
// the page-build path picks, per page, whichever of the column's nominal
// encoding and a plain RAW page yields the smaller compressed page, so a column
// is never larger than the M0 baseline and the directory records the encoding
// actually used.
//
// Every encoding works on the column's values read as uint64 through the
// column's fixed width (1, 2, 4, or 8 bytes, little-endian), so one set of
// transforms serves every integer column. A kindRaw column (the 16-byte
// resolved IP) and a width outside {1,2,4,8} stay RAW: the cascade does not
// touch opaque bytes. Float columns ride through as their bit patterns, so a
// quantized priority with few distinct values dictionaries just as an integer
// enum does.
//
// Payload shapes, all little-endian fixed ints and LEB128 varints, the fleet
// byte order. The page header's page_base (8 bytes) carries the per-page base an
// encoding subtracts, so the payload itself never repeats it.
//
//	RLE:        uvarint num_runs, then num_runs of {value width-bytes, uvarint run_len}
//	DICTIONARY: uvarint dict_size, dict_size of {value width-bytes}, u8 code_bits,
//	            bitpacked codes (num_values codes of code_bits each)
//	DELTA:      page_base = value[0]; uvarint(zigzag(value[i]-value[i-1])) for i in 1..n-1
//	FOR:        page_base = min(values); u8 bit_width, bitpacked (value[i]-min)
//	DELTA_FOR:  page_base = value[0]; u8 bit_width, uvarint delta_base,
//	            bitpacked (zigzag(value[i]-value[i-1]) - delta_base) for i in 1..n-1

// zigzag maps a signed delta to an unsigned one so a small negative step stays
// small: 0,-1,1,-2,2 -> 0,1,2,3,4.
func zigzag(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }

func unzigzag(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }

// bitWidth is the number of bits needed to hold v, 0 for v == 0.
func bitWidth(v uint64) uint8 {
	var w uint8
	for v > 0 {
		w++
		v >>= 1
	}
	return w
}

// bitpack packs vals LSB-first at a fixed width bits each. A width of 0 means
// every value is 0 and the output is empty.
func bitpack(vals []uint64, width uint8) []byte {
	if width == 0 {
		return nil
	}
	out := make([]byte, (len(vals)*int(width)+7)/8)
	bitpos := 0
	for _, v := range vals {
		for b := uint8(0); b < width; b++ {
			if v&(1<<b) != 0 {
				out[bitpos>>3] |= 1 << uint(bitpos&7)
			}
			bitpos++
		}
	}
	return out
}

// bitunpack reverses bitpack, reading n values of width bits each.
func bitunpack(b []byte, n int, width uint8) []uint64 {
	out := make([]uint64, n)
	if width == 0 {
		return out
	}
	bitpos := 0
	for i := range out {
		var v uint64
		for k := uint8(0); k < width; k++ {
			if bitpos>>3 < len(b) && b[bitpos>>3]&(1<<uint(bitpos&7)) != 0 {
				v |= 1 << k
			}
			bitpos++
		}
		out[i] = v
	}
	return out
}

// readValues reads the column-major little-endian bytes as n uint64 values.
func readValues(data []byte, width int) []uint64 {
	n := 0
	if width > 0 {
		n = len(data) / width
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = readUintWidth(data, i, width)
	}
	return out
}

// writeValues lays n uint64 values back out as column-major little-endian bytes
// of the given width, truncating each to the width's range (the values were read
// from that width to begin with).
func writeValues(vals []uint64, width int) []byte {
	out := make([]byte, len(vals)*width)
	for i, v := range vals {
		switch width {
		case 1:
			out[i] = byte(v)
		case 2:
			binary.LittleEndian.PutUint16(out[i*2:], uint16(v))
		case 4:
			binary.LittleEndian.PutUint32(out[i*4:], uint32(v))
		case 8:
			binary.LittleEndian.PutUint64(out[i*8:], v)
		}
	}
	return out
}

// encodeValues applies one encoding to the values, returning the payload and the
// page base the page header carries. It assumes enc is a real transform (not
// EncRaw) and that the values are encodable (width in {1,2,4,8}); the page
// builder handles RAW and the best-of choice.
func encodeValues(enc uint8, vals []uint64, width int) (payload []byte, pageBase uint64) {
	switch enc {
	case EncRLE:
		return encodeRLE(vals, width), 0
	case EncDict:
		return encodeDict(vals, width), 0
	case EncDelta:
		return encodeDelta(vals)
	case EncFOR:
		return encodeFOR(vals)
	case EncDeltaFOR:
		return encodeDeltaFOR(vals)
	default:
		return writeValues(vals, width), 0
	}
}

// decodeValues reverses encodeValues back to n uint64 values.
func decodeValues(enc uint8, payload []byte, n, width int, pageBase uint64) ([]uint64, error) {
	switch enc {
	case EncRaw:
		return readValues(payload, width), nil
	case EncRLE:
		return decodeRLE(payload, n, width)
	case EncDict:
		return decodeDict(payload, n, width)
	case EncDelta:
		return decodeDelta(payload, n, pageBase)
	case EncFOR:
		return decodeFOR(payload, n, pageBase)
	case EncDeltaFOR:
		return decodeDeltaFOR(payload, n, pageBase)
	default:
		return nil, errUnknownEncoding(enc)
	}
}

func putWidth(w *wbuf, v uint64, width int) {
	switch width {
	case 1:
		w.u8(uint8(v))
	case 2:
		w.b = binary.LittleEndian.AppendUint16(w.b, uint16(v))
	case 4:
		w.u32(uint32(v))
	case 8:
		w.u64(v)
	}
}

func getWidth(r *rbuf, width int) uint64 {
	switch width {
	case 1:
		return uint64(r.u8())
	case 2:
		v := r.bytes(2)
		if r.fail() {
			return 0
		}
		return uint64(binary.LittleEndian.Uint16(v))
	case 4:
		return uint64(r.u32())
	case 8:
		return r.u64()
	}
	return 0
}

// RLE: run-length, the encoding for the constant urlkey_host and the sparse
// redirect_ref where long stretches repeat.
func encodeRLE(vals []uint64, width int) []byte {
	var runs wbuf
	var w wbuf
	count := 0
	for i := 0; i < len(vals); {
		j := i + 1
		for j < len(vals) && vals[j] == vals[i] {
			j++
		}
		putWidth(&runs, vals[i], width)
		runs.uvarint(uint64(j - i))
		count++
		i = j
	}
	w.uvarint(uint64(count))
	w.bytes(runs.b)
	return w.b
}

func decodeRLE(payload []byte, n, width int) ([]uint64, error) {
	r := &rbuf{b: payload}
	runs := int(r.uvarint())
	out := make([]uint64, 0, n)
	for i := 0; i < runs; i++ {
		v := getWidth(r, width)
		ln := int(r.uvarint())
		for k := 0; k < ln; k++ {
			out = append(out, v)
		}
	}
	if r.fail() || len(out) != n {
		return nil, ErrCorrupt
	}
	return out, nil
}

// DICTIONARY: distinct values in first-appearance order, then a bitpacked code
// per row. The encoding for the small enums (status, grouping, http_status) and
// the quantized floats (priority, lambda, host_score).
func encodeDict(vals []uint64, width int) []byte {
	index := make(map[uint64]int)
	var dict []uint64
	codes := make([]uint64, len(vals))
	for i, v := range vals {
		c, ok := index[v]
		if !ok {
			c = len(dict)
			index[v] = c
			dict = append(dict, v)
		}
		codes[i] = uint64(c)
	}
	var cw uint8
	if len(dict) > 1 {
		cw = bitWidth(uint64(len(dict) - 1))
	}
	var w wbuf
	w.uvarint(uint64(len(dict)))
	for _, v := range dict {
		putWidth(&w, v, width)
	}
	w.u8(cw)
	w.bytes(bitpack(codes, cw))
	return w.b
}

func decodeDict(payload []byte, n, width int) ([]uint64, error) {
	if n == 0 {
		return make([]uint64, 0), nil
	}
	r := &rbuf{b: payload}
	dn := int(r.uvarint())
	if r.fail() || dn < 0 {
		return nil, ErrCorrupt
	}
	dict := make([]uint64, dn)
	for i := range dict {
		dict[i] = getWidth(r, width)
	}
	cw := r.u8()
	if r.fail() {
		return nil, ErrCorrupt
	}
	codes := bitunpack(r.b[r.pos:], n, cw)
	out := make([]uint64, n)
	for i, c := range codes {
		if int(c) >= len(dict) {
			return nil, ErrCorrupt
		}
		out[i] = dict[c]
	}
	return out, nil
}

// DELTA: consecutive differences, zigzagged so a non-monotone column (a
// clustered timestamp that dips) stays small. The encoding for the monotone
// url_ref and the clustered next_due, first_seen, and last_crawled times.
func encodeDelta(vals []uint64) ([]byte, uint64) {
	if len(vals) == 0 {
		return nil, 0
	}
	base := vals[0]
	var w wbuf
	prev := base
	for i := 1; i < len(vals); i++ {
		d := int64(vals[i]) - int64(prev)
		w.uvarint(zigzag(d))
		prev = vals[i]
	}
	return w.b, base
}

func decodeDelta(payload []byte, n int, base uint64) ([]uint64, error) {
	out := make([]uint64, n)
	if n == 0 {
		return out, nil
	}
	out[0] = base
	r := &rbuf{b: payload}
	prev := base
	for i := 1; i < n; i++ {
		d := unzigzag(r.uvarint())
		prev = uint64(int64(prev) + d)
		out[i] = prev
	}
	if r.fail() {
		return nil, ErrCorrupt
	}
	return out, nil
}

// FOR: frame of reference, subtract the page minimum and bitpack the residuals.
// The encoding for the small counters (crawl_count, depth, retry_count) whose
// values sit in a narrow band above zero.
func encodeFOR(vals []uint64) ([]byte, uint64) {
	if len(vals) == 0 {
		return nil, 0
	}
	min := vals[0]
	max := vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	bw := bitWidth(max - min)
	res := make([]uint64, len(vals))
	for i, v := range vals {
		res[i] = v - min
	}
	var w wbuf
	w.u8(bw)
	w.bytes(bitpack(res, bw))
	return w.b, min
}

func decodeFOR(payload []byte, n int, base uint64) ([]uint64, error) {
	if n == 0 {
		return make([]uint64, 0), nil
	}
	r := &rbuf{b: payload}
	bw := r.u8()
	if r.fail() {
		return nil, ErrCorrupt
	}
	res := bitunpack(r.b[r.pos:], n, bw)
	out := make([]uint64, n)
	for i, v := range res {
		out[i] = base + v
	}
	return out, nil
}

// DELTA_FOR: consecutive zigzag deltas, then frame-of-reference bitpack those
// deltas. The encoding for the ascending-within-host urlkey_path and the host
// table's hostkey, where the deltas are small and uniform.
func encodeDeltaFOR(vals []uint64) ([]byte, uint64) {
	if len(vals) == 0 {
		return nil, 0
	}
	base := vals[0]
	deltas := make([]uint64, len(vals)-1)
	prev := base
	for i := 1; i < len(vals); i++ {
		deltas[i-1] = zigzag(int64(vals[i]) - int64(prev))
		prev = vals[i]
	}
	var dmin uint64
	if len(deltas) > 0 {
		dmin = deltas[0]
		dmax := deltas[0]
		for _, d := range deltas {
			if d < dmin {
				dmin = d
			}
			if d > dmax {
				dmax = d
			}
		}
		bw := bitWidth(dmax - dmin)
		res := make([]uint64, len(deltas))
		for i, d := range deltas {
			res[i] = d - dmin
		}
		var w wbuf
		w.u8(bw)
		w.uvarint(dmin)
		w.bytes(bitpack(res, bw))
		return w.b, base
	}
	var w wbuf
	w.u8(0)
	w.uvarint(0)
	return w.b, base
}

func decodeDeltaFOR(payload []byte, n int, base uint64) ([]uint64, error) {
	out := make([]uint64, n)
	if n == 0 {
		return out, nil
	}
	out[0] = base
	if n == 1 {
		return out, nil
	}
	r := &rbuf{b: payload}
	bw := r.u8()
	dmin := r.uvarint()
	if r.fail() {
		return nil, ErrCorrupt
	}
	res := bitunpack(r.b[r.pos:], n-1, bw)
	prev := base
	for i := 1; i < n; i++ {
		d := unzigzag(res[i-1] + dmin)
		prev = uint64(int64(prev) + d)
		out[i] = prev
	}
	return out, nil
}
