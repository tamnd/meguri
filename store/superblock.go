package store

import (
	"encoding/binary"
	"errors"
	"os"
)

// The superblock is the two-slot checkpoint metadata, the torn-write-safe commit
// point hashlog uses (Spec 2070 doc 05). It names the current .meguri snapshot,
// the active log file, and the per-store durable frontier LSN the snapshot is
// consistent as of. A checkpoint writes the slot it did not last write, so a
// crash mid-write damages at most one slot and recovery falls back to the other:
// a crash before the new slot's fsync recovers from the old snapshot plus a
// longer log tail, a crash after recovers from the new snapshot plus a shorter
// tail, and neither loses an acknowledged write.

const (
	slotSize = 256
	sbSize   = 2 * slotSize
	sbMagic  = uint32(0x4d454755) // "MEGU"
)

// ErrNoCheckpoint marks a superblock with neither slot valid, the fresh-store
// case where there is nothing to recover from.
var ErrNoCheckpoint = errors.New("store: no valid checkpoint")

// checkpointMeta is one decoded superblock slot.
type checkpointMeta struct {
	gen      uint64 // generation, the higher valid slot wins
	frontier uint64 // durable LSN the snapshot is consistent as of
	arenaLen uint64 // string-arena length at the cut, where the post-snapshot log continues interning
	snapshot string // .meguri snapshot file name
	logName  string // active log file name
}

// encodeSlot lays one slot out in a slotSize buffer: magic, gen, frontier,
// arenaLen, the two names length-prefixed, then a crc over everything before it.
func encodeSlot(m checkpointMeta) []byte {
	b := make([]byte, slotSize)
	binary.LittleEndian.PutUint32(b[0:], sbMagic)
	binary.LittleEndian.PutUint64(b[4:], m.gen)
	binary.LittleEndian.PutUint64(b[12:], m.frontier)
	binary.LittleEndian.PutUint64(b[20:], m.arenaLen)
	i := 28
	i += putStr(b[i:], m.snapshot)
	putStr(b[i:], m.logName)
	binary.LittleEndian.PutUint32(b[slotSize-4:], crc32c(b[:slotSize-4]))
	return b
}

// decodeSlot parses a slot, returning ok=false if the magic or crc fails.
func decodeSlot(b []byte) (checkpointMeta, bool) {
	if len(b) < slotSize || binary.LittleEndian.Uint32(b[0:]) != sbMagic {
		return checkpointMeta{}, false
	}
	if crc32c(b[:slotSize-4]) != binary.LittleEndian.Uint32(b[slotSize-4:]) {
		return checkpointMeta{}, false
	}
	m := checkpointMeta{
		gen:      binary.LittleEndian.Uint64(b[4:]),
		frontier: binary.LittleEndian.Uint64(b[12:]),
		arenaLen: binary.LittleEndian.Uint64(b[20:]),
	}
	i := 28
	var n int
	m.snapshot, n = getStr(b[i:])
	i += n
	m.logName, _ = getStr(b[i:])
	return m, true
}

func putStr(b []byte, s string) int {
	binary.LittleEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return 2 + len(s)
}

func getStr(b []byte) (string, int) {
	n := int(binary.LittleEndian.Uint16(b))
	return string(b[2 : 2+n]), 2 + n
}

// readSuperblock returns the valid slot with the higher generation, or
// ErrNoCheckpoint if neither slot is valid (a fresh or never-checkpointed store).
func readSuperblock(path string) (checkpointMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return checkpointMeta{}, ErrNoCheckpoint
		}
		return checkpointMeta{}, err
	}
	if len(b) < sbSize {
		return checkpointMeta{}, ErrNoCheckpoint
	}
	a, aok := decodeSlot(b[:slotSize])
	c, cok := decodeSlot(b[slotSize:])
	switch {
	case aok && cok:
		if a.gen >= c.gen {
			return a, nil
		}
		return c, nil
	case aok:
		return a, nil
	case cok:
		return c, nil
	default:
		return checkpointMeta{}, ErrNoCheckpoint
	}
}

// writeSuperblock writes m into the slot the previous generation did not use, so
// a torn write can damage at most one slot. It fsyncs the slot before returning,
// the commit that publishes the new checkpoint.
func writeSuperblock(path string, m checkpointMeta) error {
	b := make([]byte, sbSize)
	if existing, err := os.ReadFile(path); err == nil && len(existing) >= sbSize {
		copy(b, existing[:sbSize])
	}
	// An even generation lands in slot A, an odd one in slot B, so consecutive
	// checkpoints alternate slots.
	slot := encodeSlot(m)
	if m.gen%2 == 0 {
		copy(b[:slotSize], slot)
	} else {
		copy(b[slotSize:], slot)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(b, 0); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
