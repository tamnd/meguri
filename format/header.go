package format

import "encoding/binary"

// HeaderSize is the fixed size of the .meguri header at offset 0.
const HeaderSize = 64

// Header is the fixed 64-byte file header. It lets a reader sanity-check the
// file and learn the global facts (the partition's HostKey range, the region
// offsets, the row counts) without parsing the footer, and it carries the two
// things a frontier partition file needs that a document shard does not: the
// partition id and the HostKey range the partition owns.
type Header struct {
	VersionMajor uint16
	VersionMinor uint16
	PartitionID  uint32
	Flags        uint16
	ChecksumAlgo uint8
	DefaultCodec uint8
	HostKeyLo    uint64
	HostKeyHi    uint64
	URLCount     uint64
	HostCount    uint64
	FooterOffset uint64
	CreatedHours uint32
}

// Encode writes the header into a fresh 64-byte slice, stamping the CRC32C over
// the first 60 bytes into the last 4. The magic is written first.
func (h *Header) Encode() []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], Magic[:])
	binary.LittleEndian.PutUint16(b[4:6], h.VersionMajor)
	binary.LittleEndian.PutUint16(b[6:8], h.VersionMinor)
	binary.LittleEndian.PutUint32(b[8:12], h.PartitionID)
	binary.LittleEndian.PutUint16(b[12:14], h.Flags)
	b[14] = h.ChecksumAlgo
	b[15] = h.DefaultCodec
	binary.LittleEndian.PutUint64(b[16:24], h.HostKeyLo)
	binary.LittleEndian.PutUint64(b[24:32], h.HostKeyHi)
	binary.LittleEndian.PutUint64(b[32:40], h.URLCount)
	binary.LittleEndian.PutUint64(b[40:48], h.HostCount)
	binary.LittleEndian.PutUint64(b[48:56], h.FooterOffset)
	binary.LittleEndian.PutUint32(b[56:60], h.CreatedHours)
	binary.LittleEndian.PutUint32(b[60:64], crc32c(b[0:60]))
	return b
}

// DecodeHeader parses and verifies a 64-byte header.
func DecodeHeader(b []byte) (*Header, error) {
	if len(b) < HeaderSize {
		return nil, ErrShortFile
	}
	if [4]byte(b[0:4]) != Magic {
		return nil, ErrBadMagic
	}
	if got, want := binary.LittleEndian.Uint32(b[60:64]), crc32c(b[0:60]); got != want {
		return nil, ErrChecksum
	}
	h := &Header{
		VersionMajor: binary.LittleEndian.Uint16(b[4:6]),
		VersionMinor: binary.LittleEndian.Uint16(b[6:8]),
		PartitionID:  binary.LittleEndian.Uint32(b[8:12]),
		Flags:        binary.LittleEndian.Uint16(b[12:14]),
		ChecksumAlgo: b[14],
		DefaultCodec: b[15],
		HostKeyLo:    binary.LittleEndian.Uint64(b[16:24]),
		HostKeyHi:    binary.LittleEndian.Uint64(b[24:32]),
		URLCount:     binary.LittleEndian.Uint64(b[32:40]),
		HostCount:    binary.LittleEndian.Uint64(b[40:48]),
		FooterOffset: binary.LittleEndian.Uint64(b[48:56]),
		CreatedHours: binary.LittleEndian.Uint32(b[56:60]),
	}
	if h.VersionMajor != VersionMajor {
		return nil, ErrUnsupported
	}
	return h, nil
}
