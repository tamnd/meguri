package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// The log is the durable substrate of the store, and it is the recovery journal
// in the FASTER and Bitcask sense (doc 11 section 1.2): an append is at once the
// durable data and the durable redo record, so there is no separate write-ahead
// log and no double write. A crawl update lands as one append; recovery replays
// the appends past the last checkpoint's frontier and stops at the first torn
// record. This is the kv/hashlog log-as-journal collapse (Spec 2070 doc 04
// section 3) reused wholesale; meguri adds only the frontier record codec that
// fills the frame value.
//
// Frame layout, all little-endian, the fleet byte order:
//
//	lsn   uint64   monotonic per-store sequence number, last-writer-wins on replay
//	kind  uint8    kindURL | kindHost | kindIntern, low 7 bits; bit7 set = tombstone
//	klen  uint8    key length: 16 for a URL, 8 for a host, 0 for an intern
//	vlen  uint32   value length
//	key   klen bytes
//	val   vlen bytes
//	crc   uint32   crc32c over every byte from lsn through val
//
// A frame's byte offset in the file is its logical address: the index points a
// key at the offset of its current record, and an evicted record is read back
// with one ReadAt at that offset (the hybrid log's spilled-read path).

const (
	kindURL    uint8 = 0
	kindHost   uint8 = 1
	kindIntern uint8 = 2

	kindMask uint8 = 0x7f
	tombBit  uint8 = 0x80
)

// frameHeaderSize is lsn(8) + kind(1) + klen(1) + vlen(4).
const frameHeaderSize = 14

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// ErrTornFrame marks a frame whose checksum fails: the durable tail ends here
// and replay stops, the torn-write safety net (doc 11 section 5.2).
var ErrTornFrame = errors.New("store: torn log frame")

// Durability is the three-position dial (doc 11 section 8), inherited from
// hashlog (Spec 2070 doc 04 section 5). Honesty per D19: at concurrency 1 under
// DurabilityFull one update is one device flush, the fsync floor every honest
// durable store sits on; the win is above conc-1, where group commit amortizes
// the flush across the concurrent crawl updates a partition is processing.
type Durability uint8

const (
	// DurabilityNone never fsyncs: the speed ceiling and the in-memory benchmark
	// regime, for a scratch crawl where re-deriving the frontier is cheaper than
	// paying for durability. A power loss loses everything the OS had not written
	// back (Spec 2070 doc 04 section 5.2).
	DurabilityNone Durability = iota
	// DurabilityNormal fsyncs at checkpoint boundaries, not on every update: a
	// crash loses at most the updates since the last checkpoint, and a lost crawl
	// outcome costs one redundant recrawl, not lost data. This is the default for
	// a durable frontier.
	DurabilityNormal
	// DurabilityFull fsyncs before every update returns, via group commit so
	// concurrent updates share one flush. No acknowledged update is ever lost.
	DurabilityFull
)

// log is the append-only journal file plus its sync bookkeeping.
type log struct {
	mu      sync.Mutex // serializes appends, the single-writer log discipline
	f       *os.File
	off     int64  // bytes written, the next append offset
	nextLSN uint64 // monotonic per-store sequence, assigned under mu so file order == LSN order

	written atomic.Int64 // bytes handed to the OS, read by the group-commit flusher
	syncMu  sync.Mutex   // serializes fsync so concurrent committers coalesce
	synced  int64        // bytes known durable on the device

	dur Durability
}

// openLog opens or creates the log file at path for append, positioning off at
// its current end so a reopened log continues rather than truncating.
func openLog(path string, dur Durability) (*log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	l := &log{f: f, off: size, dur: dur, nextLSN: 1}
	l.written.Store(size)
	l.synced = size
	return l, nil
}

// append writes one frame, assigning the next LSN under the same lock as the
// offset bump so file order equals LSN order, which is what lets replay rebuild
// the string arena by re-appending intern frames in their original order. The
// value and its sequence number land as one atomic record. append does not
// fsync; the caller decides durability through commit. It returns the byte
// offset the frame landed at and the LSN it was stamped with.
func (l *log) append(kind, klen uint8, key, val []byte) (int64, uint64, error) {
	frameLen := frameHeaderSize + int(klen) + len(val) + 4
	buf := make([]byte, frameLen)
	buf[8] = kind
	buf[9] = klen
	binary.LittleEndian.PutUint32(buf[10:], uint32(len(val)))
	copy(buf[frameHeaderSize:], key[:klen])
	copy(buf[frameHeaderSize+int(klen):], val)

	l.mu.Lock()
	lsn := l.nextLSN
	l.nextLSN++
	binary.LittleEndian.PutUint64(buf[0:], lsn)
	binary.LittleEndian.PutUint32(buf[frameLen-4:], crc32c(buf[:frameLen-4]))
	off := l.off
	if _, err := l.f.WriteAt(buf, off); err != nil {
		l.mu.Unlock()
		return 0, 0, err
	}
	l.off += int64(frameLen)
	l.written.Store(l.off)
	l.mu.Unlock()
	return off, lsn, nil
}

// peekLSN returns the next LSN the log will assign, the value a checkpoint
// records as its durable frontier.
func (l *log) peekLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextLSN
}

// setLSN positions the LSN counter, used on recovery to resume past the highest
// durable LSN and on rotation to continue the sequence into a fresh log.
func (l *log) setLSN(n uint64) {
	l.mu.Lock()
	l.nextLSN = n
	l.mu.Unlock()
}

// commit makes every byte written so far durable if the dial calls for it. It is
// group commit: the first caller into syncMu fsyncs everything written to that
// point, covering every concurrent appender's bytes, and a caller that finds its
// target already synced returns without a second flush. So K concurrent updates
// cost about one device flush, the amortization the conc-1 floor cannot reach.
func (l *log) commit(upTo int64) error {
	if l.dur != DurabilityFull {
		return nil
	}
	return l.fsync(upTo)
}

func (l *log) fsync(upTo int64) error {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	if l.synced >= upTo {
		return nil // a concurrent committer already flushed past us
	}
	target := l.written.Load()
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.synced = target
	return nil
}

// syncAll flushes the whole log unconditionally, the checkpoint and Normal-dial
// durability point.
func (l *log) syncAll() error { return l.fsync(l.written.Load()) }

// readAt reconstructs the key, kind, and value of the frame at off, the spilled
// read path for an evicted record. It verifies the frame's checksum so a read
// never returns a torn record's bytes as if they were live.
func (l *log) readAt(off int64) (kind, klen uint8, key, val []byte, err error) {
	var hdr [frameHeaderSize]byte
	if _, err = l.f.ReadAt(hdr[:], off); err != nil {
		return
	}
	kind = hdr[8]
	klen = hdr[9]
	vlen := binary.LittleEndian.Uint32(hdr[10:])
	rest := make([]byte, int(klen)+int(vlen)+4)
	if _, err = l.f.ReadAt(rest, off+frameHeaderSize); err != nil {
		return
	}
	frame := make([]byte, frameHeaderSize+len(rest))
	copy(frame, hdr[:])
	copy(frame[frameHeaderSize:], rest)
	if crc32c(frame[:len(frame)-4]) != binary.LittleEndian.Uint32(frame[len(frame)-4:]) {
		err = ErrTornFrame
		return
	}
	key = rest[:klen]
	val = rest[klen : int(klen)+int(vlen)]
	return
}

// frame is one decoded log record handed to a replay callback.
type frame struct {
	lsn  uint64
	kind uint8
	tomb bool
	off  int64
	key  []byte
	val  []byte
}

// replay reads the log from the start, calling fn for each intact frame in
// order, and stops cleanly at the first torn or short frame, which marks the end
// of the durable tail (doc 11 section 5.1 step 5). A short read at the very end
// is a normal stop, not an error: a crash can leave a partial trailing frame.
func (l *log) replay(fn func(frame) error) error {
	r, err := os.Open(l.f.Name())
	if err != nil {
		return err
	}
	defer r.Close()

	var off int64
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		var hdr [frameHeaderSize]byte
		n, err := io.ReadFull(br, hdr[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end, or a partial trailing header: stop
			}
			return err
		}
		_ = n
		klen := hdr[9]
		vlen := binary.LittleEndian.Uint32(hdr[10:])
		rest := make([]byte, int(klen)+int(vlen)+4)
		if _, err := io.ReadFull(br, rest); err != nil {
			return nil // a torn trailing frame: durable tail ends here
		}
		frameLen := frameHeaderSize + len(rest)
		whole := make([]byte, frameLen)
		copy(whole, hdr[:])
		copy(whole[frameHeaderSize:], rest)
		if crc32c(whole[:frameLen-4]) != binary.LittleEndian.Uint32(whole[frameLen-4:]) {
			return nil // checksum mismatch: stop at the first torn frame
		}
		f := frame{
			lsn:  binary.LittleEndian.Uint64(hdr[0:]),
			kind: hdr[8] & kindMask,
			tomb: hdr[8]&tombBit != 0,
			off:  off,
			key:  rest[:klen],
			val:  rest[klen : int(klen)+int(vlen)],
		}
		if err := fn(f); err != nil {
			return err
		}
		off += int64(frameLen)
	}
}

func (l *log) close() error { return l.f.Close() }
