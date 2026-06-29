package format

import (
	"encoding/binary"
	"sort"
)

// FSST (Fast Static Symbol Table, Boncz/Neumann/Leis) is the per-ref string
// compression doc 10 section 7 names as the refinement on top of the whole-arena
// zstd blob. zstd compresses the arena well but only as one block: a reader has to
// inflate the whole region to resolve a single URL. FSST trades a little ratio for
// random access. It trains a table of up to 255 short byte symbols over the URL
// strings, then encodes each string independently as a sequence of one-byte codes,
// so a single URL decodes on its own from the shared table without touching any
// other string. That is what the redistribution and inspection paths want when
// they resolve one host's URLs out of a fleet-sized arena.
//
// Code 255 is the escape: the next raw byte stands for itself, so a byte no symbol
// covers still round-trips. Every other code indexes the symbol table. A symbol is
// one to eight bytes; a frequent substring like "https://" or ".com/" collapses to
// a single code, which is where the ~12-20 bytes/url the doc targets comes from.

const (
	fsstMaxSymbols = 255 // codes 0..254 index the table; 255 is the escape
	fsstMaxLen     = 8   // a symbol is at most eight bytes
	fsstEscape     = 255
	fsstRounds     = 8 // training passes; the table grows longer symbols each pass
)

// fsstSymbol is one table entry: up to eight bytes and its used length.
type fsstSymbol struct {
	val [fsstMaxLen]byte
	n   uint8
}

func (s fsstSymbol) bytes() []byte { return s.val[:s.n] }

// fsstTable is a trained symbol table with the encode-side index. byFirst groups
// symbol indices by their first byte, longest first, so a match at a position
// takes the first candidate that is a prefix of the input, which is the longest.
type fsstTable struct {
	syms    []fsstSymbol
	byFirst [256][]int
}

// makeSymbol packs a byte slice (1..8 bytes) into a symbol.
func makeSymbol(b []byte) fsstSymbol {
	var s fsstSymbol
	s.n = uint8(len(b))
	copy(s.val[:], b)
	return s
}

// buildIndex fills byFirst from syms, each bucket sorted by symbol length
// descending so the first prefix match found is the longest.
func (t *fsstTable) buildIndex() {
	for i := range t.byFirst {
		t.byFirst[i] = t.byFirst[i][:0]
	}
	for i := range t.syms {
		b := t.syms[i].val[0]
		t.byFirst[b] = append(t.byFirst[b], i)
	}
	for b := range t.byFirst {
		bucket := t.byFirst[b]
		sort.Slice(bucket, func(x, y int) bool {
			return t.syms[bucket[x]].n > t.syms[bucket[y]].n
		})
	}
}

// match returns the index and length of the longest symbol that is a prefix of
// s[i:], or (-1, 0) when no symbol matches and the byte must be escaped.
func (t *fsstTable) match(s []byte, i int) (int, int) {
	for _, idx := range t.byFirst[s[i]] {
		sym := t.syms[idx]
		n := int(sym.n)
		if i+n > len(s) {
			continue
		}
		if string(sym.val[:n]) == string(s[i:i+n]) {
			return idx, n
		}
	}
	return -1, 0
}

// encode compresses one string to a code stream the table decodes on its own.
func (t *fsstTable) encode(s []byte) []byte {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		idx, n := t.match(s, i)
		if idx < 0 {
			out = append(out, fsstEscape, s[i])
			i++
			continue
		}
		out = append(out, byte(idx))
		i += n
	}
	return out
}

// decode expands one code stream back to the original bytes, reading only the
// shared table, so a single string decompresses without the rest of the arena.
func (t *fsstTable) decode(code []byte) []byte {
	out := make([]byte, 0, len(code)*2)
	for i := 0; i < len(code); {
		c := code[i]
		i++
		if c == fsstEscape {
			if i < len(code) {
				out = append(out, code[i])
				i++
			}
			continue
		}
		if int(c) < len(t.syms) {
			out = append(out, t.syms[c].bytes()...)
		}
	}
	return out
}

// tokenize walks one string under the current table, calling visit with each
// symbol's bytes (a single byte when nothing longer matches), the counting pass
// training uses.
func (t *fsstTable) tokenize(s []byte, visit func(tok []byte)) {
	for i := 0; i < len(s); {
		idx, n := t.match(s, i)
		if idx < 0 {
			visit(s[i : i+1])
			i++
			continue
		}
		visit(s[i : i+n])
		i += n
	}
}

// trainFSST builds a symbol table over the sample strings. It starts empty, so
// the first pass counts single bytes and adjacent byte pairs, and each later pass
// re-tokenizes under the growing table and proposes longer symbols (a used symbol
// concatenated with the next), keeping the 255 highest-gain candidates by
// frequency times length. The selection breaks ties lexically so the table is
// deterministic, which the gate and the on-disk format both need.
func trainFSST(samples [][]byte) *fsstTable {
	t := &fsstTable{}
	t.buildIndex()
	for range fsstRounds {
		gain := map[string]int{}
		for _, s := range samples {
			var prev []byte
			t.tokenize(s, func(tok []byte) {
				// A symbol's worth is the bytes it covers per use; the higher the
				// product of frequency and length, the more codes it saves.
				gain[string(tok)] += len(tok)
				// Propose the merge of this symbol with the one before it, so a run of
				// short symbols collapses into a longer one over the passes, which is
				// how "https://" and ".com/" emerge as single codes.
				if len(prev) > 0 && len(prev)+len(tok) <= fsstMaxLen {
					cat := string(prev) + string(tok)
					gain[cat] += len(cat)
				}
				prev = tok
			})
		}
		t.syms = topSymbols(gain)
		t.buildIndex()
	}
	return t
}

// topSymbols picks the fsstMaxSymbols highest-gain candidate strings, gain first
// then lexical, and packs them into symbols. Single bytes are always kept in the
// running so a table can still cover any byte cheaply when a string is all rare
// substrings.
func topSymbols(gain map[string]int) []fsstSymbol {
	type cand struct {
		s string
		g int
	}
	cands := make([]cand, 0, len(gain))
	for s, g := range gain {
		cands = append(cands, cand{s, g})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].g != cands[j].g {
			return cands[i].g > cands[j].g
		}
		return cands[i].s < cands[j].s
	})
	n := min(len(cands), fsstMaxSymbols)
	syms := make([]fsstSymbol, 0, n)
	for _, c := range cands[:n] {
		syms = append(syms, makeSymbol([]byte(c.s)))
	}
	return syms
}

// encodeTable serializes the symbol table: a uvarint count, then each symbol as a
// length byte and its bytes. The table is small (at most 255 symbols of 8 bytes)
// and ships once per file, shared by every encoded string.
func encodeTable(t *fsstTable) []byte {
	out := binary.AppendUvarint(nil, uint64(len(t.syms)))
	for _, s := range t.syms {
		out = append(out, s.n)
		out = append(out, s.bytes()...)
	}
	return out
}

// FSSTArena is a per-ref string arena: a shared symbol table and a span region
// where each string is stored as its own FSST code stream behind a uvarint length.
// A ref is a byte offset into the spans, and Read decodes just that string from
// the shared table, the random access the whole-arena zstd blob cannot give. It is
// the doc 10 section 7 layout: the columns keep byte-offset refs, but a resolve
// touches one span, not the whole region.
type FSSTArena struct {
	table []byte // serialized symbol table, shared by every span
	spans []byte // per-string [uvarint codelen][code] records
	t     *fsstTable
}

// BuildFSSTArena trains a table over the strings and lays each one out as an
// independently-decodable span, returning the arena and the per-string offsets in
// input order. Offset 0 is the none sentinel (a single zero byte), so a zero ref
// reads back empty, matching the verbatim arena's convention.
func BuildFSSTArena(strs [][]byte) (*FSSTArena, []uint64) {
	t := trainFSST(strs)
	a := &FSSTArena{t: t, table: encodeTable(t), spans: []byte{0}}
	offs := make([]uint64, len(strs))
	for i, s := range strs {
		offs[i] = uint64(len(a.spans))
		code := t.encode(s)
		a.spans = binary.AppendUvarint(a.spans, uint64(len(code)))
		a.spans = append(a.spans, code...)
	}
	return a, offs
}

// Read decodes the string at a span offset, reading only that span and the shared
// table. A zero or out-of-range offset reads back nil, the none sentinel.
func (a *FSSTArena) Read(off uint64) []byte {
	if off == 0 || off >= uint64(len(a.spans)) {
		return nil
	}
	n, k := binary.Uvarint(a.spans[off:])
	if k <= 0 {
		return nil
	}
	start := off + uint64(k)
	end := start + n
	if end > uint64(len(a.spans)) {
		return nil
	}
	return a.t.decode(a.spans[start:end])
}

// Bytes returns the arena's two regions, the shared table and the span region, the
// sizes a measurement sums and the file would frame as its string region.
func (a *FSSTArena) Bytes() (table, spans []byte) { return a.table, a.spans }

// LoadFSSTArena reconstructs an arena from its serialized table and span region,
// the read side a file uses after decoding the two regions off disk.
func LoadFSSTArena(table, spans []byte) *FSSTArena {
	t, _ := decodeTable(table)
	return &FSSTArena{table: table, spans: spans, t: t}
}

// decodeTable rebuilds a table and its encode index from encodeTable's bytes.
func decodeTable(b []byte) (*fsstTable, int) {
	count, k := binary.Uvarint(b)
	if k <= 0 {
		return &fsstTable{}, 0
	}
	off := k
	t := &fsstTable{syms: make([]fsstSymbol, 0, count)}
	for range count {
		if off >= len(b) {
			break
		}
		n := int(b[off])
		off++
		if n > fsstMaxLen || off+n > len(b) {
			break
		}
		t.syms = append(t.syms, makeSymbol(b[off:off+n]))
		off += n
	}
	t.buildIndex()
	return t, off
}
