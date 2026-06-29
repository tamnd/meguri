package format

import (
	"errors"
	"fmt"
)

// Sentinel errors a caller can match with errors.Is.
var (
	ErrBadMagic    = errors.New("meguri/format: bad magic")
	ErrShortFile   = errors.New("meguri/format: file too short")
	ErrCorrupt     = errors.New("meguri/format: corrupt file")
	ErrChecksum    = errors.New("meguri/format: checksum mismatch")
	ErrUnsupported = errors.New("meguri/format: unsupported version")
	ErrNotSorted   = errors.New("meguri/format: url records not sorted by urlkey")
)

func errUnknownCodec(c uint8) error {
	return fmt.Errorf("meguri/format: unknown block codec %d: %w", c, ErrCorrupt)
}

func errUnknownEncoding(e uint8) error {
	return fmt.Errorf("meguri/format: unknown column encoding %d: %w", e, ErrCorrupt)
}
