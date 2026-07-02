// Package seed is the meguri binary seed format, .seed, the raw block-framed
// corpus input that replaces gzipped JSONL for the scale passes (Spec 2074 doc
// 08). It is splittable at block boundaries, seekable by a per-block first-key
// index, and parse-free: a reader pulls a URL string with a uvarint length read
// and no JSON, and a worker handed a disjoint block range shares nothing with the
// others. It holds only URL bytes; the URLKey is derived at ingest as before.
package seed

import (
	"encoding/binary"
	"errors"
)

// The on-disk shape is a 64-byte header, a run of blocks, and a footer block
// index, with an 8-byte trailer (footer length and CRC) at the very end.
const (
	// HeaderSize is the fixed header length. It sits at offset zero so a reader
	// learns the geometry in one read.
	HeaderSize = 64
	// trailerSize is the footer length plus CRC written at end of file.
	trailerSize = 8
	// DefaultBlockSize is the block granularity, 1 MiB. A block holds thousands of
	// adjacent sorted URLs, which is a good split unit and, under the zstd codec, a
	// good compression window.
	DefaultBlockSize = 1 << 20
	// formatVersion is bumped when the layout changes; a reader rejects a version
	// it does not know rather than misreading.
	formatVersion = 1
)

// Codec selects how a block body is stored. Raw is the default: zero decode CPU,
// fixed block offsets, the simplest splittable form. Zstd compresses each block
// independently so the split survives while the file stays small.
type Codec uint8

const (
	CodecRaw  Codec = 0
	CodecZstd Codec = 1
)

// Magic marks a .seed file.
var Magic = [4]byte{'S', 'E', 'E', 'D'}

var (
	ErrShortFile    = errors.New("seed: file shorter than header")
	ErrBadMagic     = errors.New("seed: bad magic")
	ErrVersion      = errors.New("seed: unknown format version")
	ErrCorrupt      = errors.New("seed: corrupt file")
	ErrChecksum     = errors.New("seed: footer checksum mismatch")
	ErrRecordTooBig = errors.New("seed: record does not fit in a block")
)

// header is the parsed 64-byte file header.
type header struct {
	codec        Codec
	blockSize    uint32
	recordCount  uint64
	urlBytes     uint64
	hostLo       uint64
	hostHi       uint64
	blockCount   uint32
	footerOffset uint64
}

// encodeHeader writes h into a 64-byte block. The layout, all little-endian:
//
//	0:4   magic
//	4     version
//	5     codec
//	6:8   reserved
//	8:12  blockSize
//	12:16 reserved
//	16:24 recordCount
//	24:32 urlBytes
//	32:40 hostLo
//	40:48 hostHi
//	48:52 blockCount
//	52:56 reserved
//	56:64 footerOffset
func encodeHeader(h header) []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], Magic[:])
	b[4] = formatVersion
	b[5] = byte(h.codec)
	binary.LittleEndian.PutUint32(b[8:12], h.blockSize)
	binary.LittleEndian.PutUint64(b[16:24], h.recordCount)
	binary.LittleEndian.PutUint64(b[24:32], h.urlBytes)
	binary.LittleEndian.PutUint64(b[32:40], h.hostLo)
	binary.LittleEndian.PutUint64(b[40:48], h.hostHi)
	binary.LittleEndian.PutUint32(b[48:52], h.blockCount)
	binary.LittleEndian.PutUint64(b[56:64], h.footerOffset)
	return b
}

// decodeHeader parses a 64-byte header, checking magic and version.
func decodeHeader(b []byte) (header, error) {
	if len(b) < HeaderSize {
		return header{}, ErrShortFile
	}
	if [4]byte{b[0], b[1], b[2], b[3]} != Magic {
		return header{}, ErrBadMagic
	}
	if b[4] != formatVersion {
		return header{}, ErrVersion
	}
	h := header{
		codec:        Codec(b[5]),
		blockSize:    binary.LittleEndian.Uint32(b[8:12]),
		recordCount:  binary.LittleEndian.Uint64(b[16:24]),
		urlBytes:     binary.LittleEndian.Uint64(b[24:32]),
		hostLo:       binary.LittleEndian.Uint64(b[32:40]),
		hostHi:       binary.LittleEndian.Uint64(b[40:48]),
		blockCount:   binary.LittleEndian.Uint32(b[48:52]),
		footerOffset: binary.LittleEndian.Uint64(b[56:64]),
	}
	if h.codec != CodecRaw && h.codec != CodecZstd {
		return header{}, ErrCorrupt
	}
	if h.blockSize == 0 {
		return header{}, ErrCorrupt
	}
	return h, nil
}

// blockMeta is one footer index entry: where the block body lives, how many
// records it holds, and its first record's URL bytes, the seek key. offset and
// compLen are meaningful for the zstd codec, where blocks are variable-length; for
// raw they are implied by blockSize and left zero.
type blockMeta struct {
	offset   uint64
	compLen  uint32
	records  uint32
	firstKey []byte
}

// encodeFooter serializes the block index. Each entry is
// offset(uvarint) compLen(uvarint) records(uvarint) keyLen(uvarint) key.
func encodeFooter(blocks []blockMeta) []byte {
	var b []byte
	for _, m := range blocks {
		b = binary.AppendUvarint(b, m.offset)
		b = binary.AppendUvarint(b, uint64(m.compLen))
		b = binary.AppendUvarint(b, uint64(m.records))
		b = binary.AppendUvarint(b, uint64(len(m.firstKey)))
		b = append(b, m.firstKey...)
	}
	return b
}

// decodeFooter parses n block index entries from b.
func decodeFooter(b []byte, n int) ([]blockMeta, error) {
	blocks := make([]blockMeta, 0, n)
	pos := 0
	for range n {
		off, k := binary.Uvarint(b[pos:])
		if k <= 0 {
			return nil, ErrCorrupt
		}
		pos += k
		cl, k := binary.Uvarint(b[pos:])
		if k <= 0 {
			return nil, ErrCorrupt
		}
		pos += k
		rc, k := binary.Uvarint(b[pos:])
		if k <= 0 {
			return nil, ErrCorrupt
		}
		pos += k
		kl, k := binary.Uvarint(b[pos:])
		if k <= 0 {
			return nil, ErrCorrupt
		}
		pos += k
		if pos+int(kl) > len(b) {
			return nil, ErrCorrupt
		}
		key := b[pos : pos+int(kl)]
		pos += int(kl)
		blocks = append(blocks, blockMeta{
			offset:   off,
			compLen:  uint32(cl),
			records:  uint32(rc),
			firstKey: key,
		})
	}
	return blocks, nil
}
