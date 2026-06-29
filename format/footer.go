package format

import (
	"math"
	"sort"
)

// Footer section tags. Each section is framed as uvarint(tag), uvarint(length),
// body. Unknown tags are skipped by length so a newer writer's extra sections do
// not break an older reader.
const (
	secRegions      uint64 = 1
	secURLColumns   uint64 = 2
	secHostColumns  uint64 = 3
	secSchedule     uint64 = 4 // reserved, written from M1
	secSeenset      uint64 = 5 // reserved, written from M2
	secBlob         uint64 = 6 // reserved
	secStats        uint64 = 7
	secKeyValueMeta uint64 = 8
)

// regionDesc locates one top-level region in the file and carries its checksum.
type regionDesc struct {
	id     uint8
	offset uint64
	length uint64
	crc    uint32
	flags  uint8
}

// statsBlock is the at-a-glance summary inspect prints without touching the
// column data.
type statsBlock struct {
	urlCount          uint64
	hostCount         uint64
	hostKeyLo         uint64
	hostKeyHi         uint64
	scheduledCount    uint64
	dueMin            uint32
	dueMax            uint32
	totalCompressed   uint64
	totalUncompressed uint64
	bytesPerURL       float32
}

// kvPair is one entry of the optional string metadata section.
type kvPair struct {
	key string
	val string
}

// footerData is the parsed footer: the region directory, the two column
// directories, the stats, and any string metadata.
type footerData struct {
	regions []regionDesc
	urlDir  []columnDir
	hostDir []columnDir
	stats   statsBlock
	meta    []kvPair
}

// columnDir flags.
const dirFlagHasZone uint8 = 1 << 0

// encodeFooter serializes the footer sections block. The output is deterministic:
// region and column directories preserve insertion order, and metadata keys are
// sorted, so the same footerData always produces the same bytes.
func encodeFooter(f *footerData) []byte {
	var w wbuf

	// REGIONS
	{
		var s wbuf
		s.uvarint(uint64(len(f.regions)))
		for _, r := range f.regions {
			s.u8(r.id)
			s.u64(r.offset)
			s.u64(r.length)
			s.u32(r.crc)
			s.u8(r.flags)
		}
		writeSection(&w, secRegions, s.b)
	}

	writeSection(&w, secURLColumns, encodeColumnDir(f.urlDir))
	writeSection(&w, secHostColumns, encodeColumnDir(f.hostDir))

	// STATS
	{
		var s wbuf
		s.u64(f.stats.urlCount)
		s.u64(f.stats.hostCount)
		s.u64(f.stats.hostKeyLo)
		s.u64(f.stats.hostKeyHi)
		s.u64(f.stats.scheduledCount)
		s.u32(f.stats.dueMin)
		s.u32(f.stats.dueMax)
		s.u64(f.stats.totalCompressed)
		s.u64(f.stats.totalUncompressed)
		s.b = appF32(s.b, f.stats.bytesPerURL)
		writeSection(&w, secStats, s.b)
	}

	// KEY_VALUE_META, only when present, keys sorted for determinism.
	if len(f.meta) > 0 {
		meta := append([]kvPair(nil), f.meta...)
		sort.Slice(meta, func(i, j int) bool { return meta[i].key < meta[j].key })
		var s wbuf
		s.uvarint(uint64(len(meta)))
		for _, kv := range meta {
			s.uvarint(uint64(len(kv.key)))
			s.bytes([]byte(kv.key))
			s.uvarint(uint64(len(kv.val)))
			s.bytes([]byte(kv.val))
		}
		writeSection(&w, secKeyValueMeta, s.b)
	}

	return w.b
}

// writeSection frames one section into w.
func writeSection(w *wbuf, tag uint64, body []byte) {
	w.uvarint(tag)
	w.uvarint(uint64(len(body)))
	w.bytes(body)
}

// encodeColumnDir serializes a column directory body.
func encodeColumnDir(dir []columnDir) []byte {
	var s wbuf
	s.uvarint(uint64(len(dir)))
	for _, d := range dir {
		s.uvarint(uint64(d.columnID))
		s.u64(d.firstPageOffset)
		s.u64(d.totalCompressed)
		s.u64(d.totalUncompressed)
		s.uvarint(d.numValues)
		s.uvarint(d.numPages)
		s.u8(d.width)
		s.u8(d.encoding)
		s.u8(d.codec)
		s.u64(d.totalUncompressed) // uncompressed_size, logical column size
		s.u32(d.columnCRC32C)
		var flags uint8
		if d.hasZone {
			flags |= dirFlagHasZone
		}
		s.u8(flags)
		s.u64(d.zoneMin)
		s.u64(d.zoneMax)
		if d.numPages > 1 {
			// Multi-page column: the page_index_offset slot carries the inline per-page
			// skip list rather than a placeholder. A single-page column keeps the M0
			// u64(0), so a partition that did not opt into splitting is byte-identical.
			encodePageSkipList(&s, d.pages)
		} else {
			s.u64(0) // page_index_offset, unused for a single-page column
		}
	}
	return s.b
}

// encodePageSkipList serializes a multi-page column's per-page skip list. The
// count is implied by the directory's numPages, so it is not repeated here.
func encodePageSkipList(s *wbuf, pages []pageEntry) {
	for _, pe := range pages {
		s.u64(pe.firstRow)
		s.u64(pe.numValues)
		s.u64(pe.byteLen)
		s.u8(pe.encoding)
		var pf uint8
		if pe.hasZone {
			pf = 1
		}
		s.u8(pf)
		s.u64(pe.zoneMin)
		s.u64(pe.zoneMax)
	}
}

// decodeFooter parses a footer sections block. Unknown section tags are skipped.
func decodeFooter(b []byte) (*footerData, error) {
	r := &rbuf{b: b}
	f := &footerData{}
	for r.pos < len(b) && !r.fail() {
		tag := r.uvarint()
		n := int(r.uvarint())
		if r.fail() {
			break
		}
		body := r.bytes(n)
		if r.fail() {
			break
		}
		switch tag {
		case secRegions:
			f.regions = decodeRegions(body)
		case secURLColumns:
			f.urlDir = decodeColumnDir(body)
		case secHostColumns:
			f.hostDir = decodeColumnDir(body)
		case secStats:
			f.stats = decodeStats(body)
		case secKeyValueMeta:
			f.meta = decodeMeta(body)
		default:
			// reserved or newer section, already skipped by length
		}
	}
	if r.fail() {
		return nil, r.err
	}
	return f, nil
}

func decodeRegions(b []byte) []regionDesc {
	r := &rbuf{b: b}
	n := int(r.uvarint())
	out := make([]regionDesc, 0, n)
	for range n {
		out = append(out, regionDesc{
			id:     r.u8(),
			offset: r.u64(),
			length: r.u64(),
			crc:    r.u32(),
			flags:  r.u8(),
		})
	}
	return out
}

func decodeColumnDir(b []byte) []columnDir {
	r := &rbuf{b: b}
	n := int(r.uvarint())
	out := make([]columnDir, 0, n)
	for range n {
		d := columnDir{}
		d.columnID = int(r.uvarint())
		d.firstPageOffset = r.u64()
		d.totalCompressed = r.u64()
		d.totalUncompressed = r.u64()
		d.numValues = r.uvarint()
		d.numPages = r.uvarint()
		d.width = r.u8()
		d.encoding = r.u8()
		d.codec = r.u8()
		_ = r.u64() // uncompressed_size mirror
		d.columnCRC32C = r.u32()
		flags := r.u8()
		d.hasZone = flags&dirFlagHasZone != 0
		d.zoneMin = r.u64()
		d.zoneMax = r.u64()
		if d.numPages > 1 {
			d.pages = decodePageSkipList(r, int(d.numPages))
		} else {
			_ = r.u64() // page_index_offset
		}
		out = append(out, d)
	}
	return out
}

// decodePageSkipList reads a multi-page column's per-page skip list, n entries,
// written by encodePageSkipList in place of the single-page page_index_offset.
func decodePageSkipList(r *rbuf, n int) []pageEntry {
	out := make([]pageEntry, 0, n)
	for range n {
		pe := pageEntry{
			firstRow:  r.u64(),
			numValues: r.u64(),
			byteLen:   r.u64(),
			encoding:  r.u8(),
		}
		pe.hasZone = r.u8()&1 != 0
		pe.zoneMin = r.u64()
		pe.zoneMax = r.u64()
		out = append(out, pe)
	}
	return out
}

func decodeStats(b []byte) statsBlock {
	r := &rbuf{b: b}
	s := statsBlock{
		urlCount:          r.u64(),
		hostCount:         r.u64(),
		hostKeyLo:         r.u64(),
		hostKeyHi:         r.u64(),
		scheduledCount:    r.u64(),
		dueMin:            r.u32(),
		dueMax:            r.u32(),
		totalCompressed:   r.u64(),
		totalUncompressed: r.u64(),
	}
	s.bytesPerURL = math.Float32frombits(r.u32())
	return s
}

func decodeMeta(b []byte) []kvPair {
	r := &rbuf{b: b}
	n := int(r.uvarint())
	out := make([]kvPair, 0, n)
	for range n {
		kl := int(r.uvarint())
		k := string(r.bytes(kl))
		vl := int(r.uvarint())
		v := string(r.bytes(vl))
		out = append(out, kvPair{key: k, val: v})
	}
	return out
}
