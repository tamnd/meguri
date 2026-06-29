package format

import (
	"hash/crc32"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Magic brackets a .meguri file at both ends.
var Magic = [4]byte{'M', 'E', 'G', '1'}

// Format version. Major changes break the layout; minor changes are additive.
const (
	VersionMajor uint16 = 1
	VersionMinor uint16 = 0
)

// Checksum algorithm selectors, stored in the header's checksum_algo byte.
const (
	ChecksumNone   uint8 = 0
	ChecksumCRC32C uint8 = 1
	ChecksumXXH64  uint8 = 2
)

// Block codec ids, stored in the header's default_codec byte and per page. The
// values match tatami's codec enum so a shared decoder can serve both formats.
const (
	CodecNone     uint8 = 0
	CodecLZ4      uint8 = 1 // reserved, not wired in M0
	CodecZstd     uint8 = 2
	CodecZstdDict uint8 = 3 // reserved, not wired in M0
)

// Column encoding ids, stored per page and as the dominant encoding per column.
// The values match tatami's encoding enum. M0 writes RAW only; the others are
// decoded-ready placeholders for later milestones.
const (
	EncRaw      uint8 = 0
	EncDict     uint8 = 1
	EncDelta    uint8 = 2
	EncFOR      uint8 = 3
	EncRLE      uint8 = 4
	EncFSST     uint8 = 5
	EncDeltaFOR uint8 = 6
)

// Header flag bits.
const (
	FlagSorted           uint16 = 1 << 0
	FlagHasSchedule      uint16 = 1 << 1
	FlagHasSeenset       uint16 = 1 << 2
	FlagHasBlob          uint16 = 1 << 3
	FlagSeensetIsRibbon  uint16 = 1 << 4
	FlagHasMPHF          uint16 = 1 << 5
	FlagFooterCompressed uint16 = 1 << 6
)

// Region ids, the fixed order regions appear in a .meguri file.
const (
	RegionURLTable   uint8 = 0
	RegionHostTable  uint8 = 1
	RegionSchedule   uint8 = 2
	RegionSeenset    uint8 = 3
	RegionStringBlob uint8 = 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// crc32c returns the CRC32C (Castagnoli) of b, the default checksum primitive,
// hardware-accelerated on amd64 and arm64 through the standard library.
func crc32c(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// A single shared zstd encoder and decoder. The encoder is fixed at the default
// level with concurrency 1 so EncodeAll is deterministic for a given build: the
// same input always produces the same compressed bytes, which is what the
// byte-stable round-trip gate requires.
var (
	zstdEncOnce sync.Once
	zstdDecOnce sync.Once
	zstdEnc     *zstd.Encoder
	zstdDec     *zstd.Decoder
)

func encoder() *zstd.Encoder {
	zstdEncOnce.Do(func() {
		zstdEnc, _ = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
		)
	})
	return zstdEnc
}

func decoder() *zstd.Decoder {
	zstdDecOnce.Do(func() {
		zstdDec, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	})
	return zstdDec
}

// compress applies the block codec to src, returning the codec actually used
// and the output bytes. CodecNone returns src unchanged.
func compress(codec uint8, src []byte) (uint8, []byte) {
	switch codec {
	case CodecZstd:
		return CodecZstd, encoder().EncodeAll(src, make([]byte, 0, len(src)))
	default:
		return CodecNone, src
	}
}

// decompress reverses compress. uncompressedSize bounds the output so a corrupt
// length cannot drive an unbounded allocation.
func decompress(codec uint8, src []byte, uncompressedSize int) ([]byte, error) {
	switch codec {
	case CodecZstd:
		return decoder().DecodeAll(src, make([]byte, 0, uncompressedSize))
	case CodecNone:
		return src, nil
	default:
		return nil, errUnknownCodec(codec)
	}
}
