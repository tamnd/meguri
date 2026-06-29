package format

import (
	"encoding/binary"
	"math"
)

// colKind classifies a column for zone-map purposes. M0 encodes every column
// RAW; the kind only decides whether a numeric min/max zone map is meaningful.
type colKind uint8

const (
	kindUint  colKind = iota // unsigned integer, width 1/2/4/8, zone-mapped
	kindFloat                // f32 bit pattern, no zone map in M0
	kindRaw                  // fixed-width opaque bytes (e.g. a 16-byte IP)
)

// column is one table column ready to serialize: its schema id, the per-value
// width, its kind, the block codec to apply, and the column-major little-endian
// bytes.
type column struct {
	id    int
	width int
	kind  colKind
	codec uint8
	data  []byte
}

// numValues returns the row count the column holds.
func (c column) numValues() int {
	if c.width == 0 {
		return 0
	}
	return len(c.data) / c.width
}

// columnDir is one footer column-directory entry, locating a column's pages and
// summarizing what is in them.
type columnDir struct {
	columnID          int
	firstPageOffset   uint64
	totalCompressed   uint64
	totalUncompressed uint64
	numValues         uint64
	numPages          uint64
	encoding          uint8
	codec             uint8
	columnCRC32C      uint32
	zoneMin           uint64
	zoneMax           uint64
	hasZone           bool
}

// encodeColumnRegion lays the columns out as one RAW data page each and returns
// the region bytes plus the directory. regionStart is the region's absolute
// offset from the start of the file, so the directory's firstPageOffset is
// absolute, matching the spec. A column with zero rows still gets a page so the
// directory entry and the decode path stay uniform.
func encodeColumnRegion(cols []column, regionStart uint64) ([]byte, []columnDir) {
	var region []byte
	dir := make([]columnDir, 0, len(cols))
	for _, c := range cols {
		pageOff := regionStart + uint64(len(region))
		page := writePage(PageData, EncRaw, c.codec, uint32(c.numValues()), 0, 0, c.data)
		region = append(region, page...)
		zmin, zmax, hasZone := zoneMap(c)
		dir = append(dir, columnDir{
			columnID:          c.id,
			firstPageOffset:   pageOff,
			totalCompressed:   uint64(len(page) - PageHeaderSize),
			totalUncompressed: uint64(len(c.data)),
			numValues:         uint64(c.numValues()),
			numPages:          1,
			encoding:          EncRaw,
			codec:             c.codec,
			columnCRC32C:      crc32c(page),
			zoneMin:           zmin,
			zoneMax:           zmax,
			hasZone:           hasZone,
		})
	}
	return region, dir
}

// decodeColumnRegion reads the columns a directory locates out of the file
// bytes, verifying each column's CRC and each page's CRC, and returns
// columnID -> decoded column-major bytes.
func decodeColumnRegion(file []byte, dir []columnDir) (map[int][]byte, error) {
	out := make(map[int][]byte, len(dir))
	for _, d := range dir {
		off := int(d.firstPageOffset)
		if off < 0 || off > len(file) {
			return nil, ErrCorrupt
		}
		var data []byte
		var pageSpan []byte
		cur := off
		for p := uint64(0); p < d.numPages; p++ {
			if cur > len(file) {
				return nil, ErrCorrupt
			}
			h, payload, consumed, err := readPage(file[cur:])
			if err != nil {
				return nil, err
			}
			if h.encoding != EncRaw {
				return nil, errUnknownEncoding(h.encoding)
			}
			data = append(data, payload...)
			pageSpan = append(pageSpan, file[cur:cur+consumed]...)
			cur += consumed
		}
		if crc32c(pageSpan) != d.columnCRC32C {
			return nil, ErrChecksum
		}
		out[d.columnID] = data
	}
	return out, nil
}

// zoneMap computes a column-level min/max for an unsigned-integer column. Float
// and opaque-byte columns carry no zone map in M0.
func zoneMap(c column) (min, max uint64, ok bool) {
	if c.kind != kindUint {
		return 0, 0, false
	}
	n := c.numValues()
	if n == 0 {
		return 0, 0, false
	}
	min, max = math.MaxUint64, 0
	for i := range n {
		v := readUintWidth(c.data, i, c.width)
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max, true
}

// readUintWidth reads the i-th unsigned value of the given width from
// column-major bytes.
func readUintWidth(data []byte, i, width int) uint64 {
	switch width {
	case 1:
		return uint64(data[i])
	case 2:
		return uint64(binary.LittleEndian.Uint16(data[i*2:]))
	case 4:
		return uint64(binary.LittleEndian.Uint32(data[i*4:]))
	case 8:
		return binary.LittleEndian.Uint64(data[i*8:])
	default:
		return 0
	}
}

// Column-major append helpers, used by the table builders.
func appU8(dst []byte, v uint8) []byte   { return append(dst, v) }
func appU16(dst []byte, v uint16) []byte { return binary.LittleEndian.AppendUint16(dst, v) }
func appU32(dst []byte, v uint32) []byte { return binary.LittleEndian.AppendUint32(dst, v) }
func appU64(dst []byte, v uint64) []byte { return binary.LittleEndian.AppendUint64(dst, v) }
func appF32(dst []byte, v float32) []byte {
	return binary.LittleEndian.AppendUint32(dst, math.Float32bits(v))
}

// Column-major read helpers.
func getU8(data []byte, i int) uint8   { return data[i] }
func getU16(data []byte, i int) uint16 { return binary.LittleEndian.Uint16(data[i*2:]) }
func getU32(data []byte, i int) uint32 { return binary.LittleEndian.Uint32(data[i*4:]) }
func getU64(data []byte, i int) uint64 { return binary.LittleEndian.Uint64(data[i*8:]) }
func getF32(data []byte, i int) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))
}
