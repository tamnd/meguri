package distribute

import (
	"encoding/binary"
	"math"
	"sort"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// batch.go is the columnar delta+FSST wire body for a discovery batch (doc 12
// section 6). The in-process channel transport passes live slices, but a fleet
// binding ships bytes over a partitioned log, and a per-link row is mostly two
// 64-bit keys and a URL string, both of which a row-major encoding stores at full
// width. This body lays the batch out column by column: the routing keys are
// delta-coded over the sorted rows so a host's links cost a small increment each,
// the per-row scalars pack as varints, and the canonical URLs share one FSST
// table with a per-row span ref, the same per-ref codec the string region uses.
//
// The batch is a set: idempotency is the receiver's seen-set, so order does not
// matter and the encoder sorts by URLKey to make the key deltas small. Decode
// returns the rows in that sorted order, which the seen-set absorbs exactly as it
// absorbs the at-least-once redelivery.

// zigzag maps a signed delta to an unsigned varint-friendly value, small
// magnitudes near zero whichever sign, so a PathKey or timestamp that steps down
// as often as up still codes short.
func zigzag(v int64) uint64   { return uint64((v << 1) ^ (v >> 63)) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// EncodeBatch serializes a discovery batch to the columnar delta+FSST body. An
// empty batch is the empty body. The rows are sorted by URLKey first, so the
// HostKey column is ascending and codes as plain deltas and the URL spans land in
// key order.
func EncodeBatch(batch []m.Discovery) []byte {
	if len(batch) == 0 {
		return nil
	}
	b := append([]m.Discovery(nil), batch...)
	sort.Slice(b, func(i, j int) bool { return b[i].URLKey.Less(b[j].URLKey) })
	n := len(b)

	urls := make([][]byte, n)
	for i := range b {
		urls[i] = []byte(b[i].CanonicalURL)
	}
	arena, offs := format.BuildFSSTArena(urls)
	table, spans := arena.Bytes()

	out := binary.AppendUvarint(nil, uint64(n))

	// HostKey: ascending, plain deltas.
	var prevH uint64
	for i := range b {
		out = binary.AppendUvarint(out, b[i].URLKey.HostKey-prevH)
		prevH = b[i].URLKey.HostKey
	}
	// PathKey: zigzag delta from the previous row, since it resets across hosts.
	var prevP int64
	for i := range b {
		cur := int64(b[i].URLKey.PathKey)
		out = binary.AppendUvarint(out, zigzag(cur-prevP))
		prevP = cur
	}
	// Depth: small uvarint.
	for i := range b {
		out = binary.AppendUvarint(out, uint64(b[i].Depth))
	}
	// DiscoverySource: one byte.
	for i := range b {
		out = append(out, byte(b[i].DiscoverySource))
	}
	// SrcHostKey: zigzag delta, since consecutive rows often share a source page.
	var prevS int64
	for i := range b {
		cur := int64(b[i].SrcHostKey)
		out = binary.AppendUvarint(out, zigzag(cur-prevS))
		prevS = cur
	}
	// LinkWeight: raw float32, high entropy, no delta helps.
	for i := range b {
		out = binary.LittleEndian.AppendUint32(out, math.Float32bits(b[i].LinkWeight))
	}
	// AnchorHint: one byte.
	for i := range b {
		out = append(out, byte(b[i].AnchorHint))
	}
	// ObservedAt: zigzag delta, clustered in time.
	var prevO int64
	for i := range b {
		cur := int64(b[i].ObservedAt)
		out = binary.AppendUvarint(out, zigzag(cur-prevO))
		prevO = cur
	}
	// URL span refs: monotonic in key order, plain deltas.
	var prevR uint64
	for i := range b {
		out = binary.AppendUvarint(out, offs[i]-prevR)
		prevR = offs[i]
	}

	out = binary.AppendUvarint(out, uint64(len(table)))
	out = append(out, table...)
	out = binary.AppendUvarint(out, uint64(len(spans)))
	out = append(out, spans...)
	return out
}

// DecodeBatch reverses EncodeBatch, returning the rows in the encoder's sorted
// order and whether the body was well-formed. A truncated or malformed body
// reports not-ok rather than returning partial rows.
func DecodeBatch(body []byte) ([]m.Discovery, bool) {
	if len(body) == 0 {
		return nil, true
	}
	p := 0
	readUv := func() (uint64, bool) {
		v, k := binary.Uvarint(body[p:])
		if k <= 0 {
			return 0, false
		}
		p += k
		return v, true
	}
	readBytes := func(n int) ([]byte, bool) {
		if n < 0 || p+n > len(body) {
			return nil, false
		}
		b := body[p : p+n]
		p += n
		return b, true
	}

	n64, ok := readUv()
	if !ok {
		return nil, false
	}
	n := int(n64)
	if n < 0 || n > len(body) {
		return nil, false
	}
	out := make([]m.Discovery, n)

	var prevH uint64
	for i := range n {
		d, ok := readUv()
		if !ok {
			return nil, false
		}
		prevH += d
		out[i].URLKey.HostKey = prevH
	}
	var prevP int64
	for i := range n {
		z, ok := readUv()
		if !ok {
			return nil, false
		}
		prevP += unzigzag(z)
		out[i].URLKey.PathKey = uint64(prevP)
	}
	for i := range n {
		v, ok := readUv()
		if !ok {
			return nil, false
		}
		out[i].Depth = uint16(v)
	}
	src, ok := readBytes(n)
	if !ok {
		return nil, false
	}
	for i := range n {
		out[i].DiscoverySource = m.DiscoverySource(src[i])
	}
	var prevS int64
	for i := range n {
		z, ok := readUv()
		if !ok {
			return nil, false
		}
		prevS += unzigzag(z)
		out[i].SrcHostKey = uint64(prevS)
	}
	weights, ok := readBytes(4 * n)
	if !ok {
		return nil, false
	}
	for i := range n {
		out[i].LinkWeight = math.Float32frombits(binary.LittleEndian.Uint32(weights[4*i:]))
	}
	anchors, ok := readBytes(n)
	if !ok {
		return nil, false
	}
	for i := range n {
		out[i].AnchorHint = m.AnchorHint(anchors[i])
	}
	var prevO int64
	for i := range n {
		z, ok := readUv()
		if !ok {
			return nil, false
		}
		prevO += unzigzag(z)
		out[i].ObservedAt = uint32(prevO)
	}
	refs := make([]uint64, n)
	var prevR uint64
	for i := range n {
		d, ok := readUv()
		if !ok {
			return nil, false
		}
		prevR += d
		refs[i] = prevR
	}

	tlen, ok := readUv()
	if !ok {
		return nil, false
	}
	table, ok := readBytes(int(tlen))
	if !ok {
		return nil, false
	}
	slen, ok := readUv()
	if !ok {
		return nil, false
	}
	spans, ok := readBytes(int(slen))
	if !ok {
		return nil, false
	}
	arena := format.LoadFSSTArena(table, spans)
	for i := range n {
		out[i].CanonicalURL = string(arena.Read(refs[i]))
	}
	return out, true
}
