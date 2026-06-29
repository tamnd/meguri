package format

import "encoding/binary"

// PageHeaderSize is the fixed, uncompressed page header size.
const PageHeaderSize = 32

// Page kinds.
const (
	PageData     uint8 = 0
	PageDict     uint8 = 1
	PageIndex    uint8 = 2
	PageBlob     uint8 = 3
	PageFilter   uint8 = 4
	PageSchedule uint8 = 5
)

// pageHeader mirrors the 32-byte on-disk page header. There is no null bitmap:
// a frontier field is never null, it has a zero sentinel where the schema says
// so, and the bytes tatami spends on null accounting are repurposed here into
// the 8-byte page_base for the delta and frame-of-reference encodings.
type pageHeader struct {
	kind             uint8
	encoding         uint8
	codec            uint8
	flags            uint8
	numValues        uint32
	uncompressedSize uint32
	compressedSize   uint32
	firstRowIndex    uint32
	pageBase         uint64
	payloadCRC32C    uint32
}

// writePage frames one page: it applies the block codec to payload, writes the
// 32-byte header, and appends the compressed payload. It returns the full page
// bytes. The codec actually used is recorded in the header, so CodecNone is
// honored when compression would not help a caller that asked for none.
func writePage(kind, encoding, codec uint8, numValues, firstRow uint32, pageBase uint64, payload []byte) []byte {
	usedCodec, comp := compress(codec, payload)
	out := make([]byte, PageHeaderSize+len(comp))
	out[0] = kind
	out[1] = encoding
	out[2] = usedCodec
	out[3] = 0 // no inline min/max in M0; zone maps live in the column directory
	binary.LittleEndian.PutUint32(out[4:8], numValues)
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(out[12:16], uint32(len(comp)))
	binary.LittleEndian.PutUint32(out[16:20], firstRow)
	binary.LittleEndian.PutUint64(out[20:28], pageBase)
	binary.LittleEndian.PutUint32(out[28:32], crc32c(comp))
	copy(out[PageHeaderSize:], comp)
	return out
}

// readPage decodes one page starting at b[0]. It verifies the payload CRC,
// decompresses, and returns the header, the decoded payload, and the number of
// bytes the whole page occupied so a caller can stride to the next page.
func readPage(b []byte) (pageHeader, []byte, int, error) {
	if len(b) < PageHeaderSize {
		return pageHeader{}, nil, 0, ErrCorrupt
	}
	h := pageHeader{
		kind:             b[0],
		encoding:         b[1],
		codec:            b[2],
		flags:            b[3],
		numValues:        binary.LittleEndian.Uint32(b[4:8]),
		uncompressedSize: binary.LittleEndian.Uint32(b[8:12]),
		compressedSize:   binary.LittleEndian.Uint32(b[12:16]),
		firstRowIndex:    binary.LittleEndian.Uint32(b[16:20]),
		pageBase:         binary.LittleEndian.Uint64(b[20:28]),
		payloadCRC32C:    binary.LittleEndian.Uint32(b[28:32]),
	}
	end := PageHeaderSize + int(h.compressedSize)
	if end > len(b) {
		return pageHeader{}, nil, 0, ErrCorrupt
	}
	comp := b[PageHeaderSize:end]
	if crc32c(comp) != h.payloadCRC32C {
		return pageHeader{}, nil, 0, ErrChecksum
	}
	payload, err := decompress(h.codec, comp, int(h.uncompressedSize))
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	if uint32(len(payload)) != h.uncompressedSize {
		return pageHeader{}, nil, 0, ErrCorrupt
	}
	return h, payload, end, nil
}
