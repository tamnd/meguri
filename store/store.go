// Package store is the durable live state store of a partition (doc 11): the hot
// mutable form of the frontier, log-structured so a crawl update is an append and
// the log is its own recovery journal (D11). It reuses the kv/hashlog design
// (Spec 2070): a resident sharded hash index over an append-only log, the
// log-as-journal collapse with no separate write-ahead log, group commit, the
// three-position durability dial, and larger-than-memory residency where the hot
// tail is in RAM and the cold bulk is on disk addressed by log offset.
//
// meguri adds the frontier-specific layer: the URLRecord and HostRecord codec
// that fills the log frame value, the string arena the records' references index
// into, and the checkpoint that folds the live state into a .meguri file (doc 10)
// plus the durable log frontier (D15). Recovery loads that snapshot, rebuilds the
// index, and replays the log tail past the snapshot's frontier, per-partition and
// redo-only: the log is append-only, so there is nothing to roll back.
//
// The single-file promise holds (D20): open a directory, get a partition. The
// store keeps one active log, one .meguri snapshot, and a two-slot superblock,
// all pure Go, CGO_ENABLED=0, no internal/ directories.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/drum"
	"github.com/tamnd/meguri/format"
)

// numShards is the URL index shard count, the hashlog default (Spec 2070): a
// power of two so the shard is a mask of the HostKey, and large enough that
// writers on different hosts rarely share a shard lock.
const numShards = 256

// urlShard is one stripe of the URL index: a map from URLKey to the location of
// its current record, guarded by its own lock so unrelated keys never contend.
type urlShard struct {
	mu sync.RWMutex
	m  map[meguri.URLKey]*urlLoc
}

// urlLoc is the index entry for a URL: the log offset of its current frame, its
// LSN, and the decoded record when resident. A nil rec means the record is
// evicted to disk and a read re-materializes it from the log at off (the hybrid
// log's spilled-read path, doc 11 section 6.2).
type urlLoc struct {
	off  int64
	lsn  uint64
	rec  *meguri.URLRecord
	tomb bool
}

// hostLoc is the index entry for a host. Hosts are few, so the host table stays
// fully resident (doc 11 section 6.1).
type hostLoc struct {
	off  int64
	lsn  uint64
	rec  meguri.HostRecord
	tomb bool
}

// Store is the durable live frontier of one partition.
type Store struct {
	dir string
	dur Durability

	log     *log
	logName string
	gen     uint64

	arenaMu sync.Mutex
	arena   []byte // string region, uvarint-length-prefixed entries, offset 0 reserved

	// Stage A spilled arena (spec 2072 doc 05): when set, the canonical-URL strings
	// are not kept resident in arena above; they live in a derived spill file read
	// by byte offset through a bounded LRU (spill), and arenaLen is the next offset
	// the file will assign. arena stays nil in this mode. The spill file is a pure
	// cache, rebuilt at Open from the snapshot string region plus the kindIntern log
	// frames, so its durability is the log's, not its own.
	spill        *spilledArena
	spillFile    *os.File
	arenaLen     int64  // next offset the spill file will assign (logical length)
	arenaFlushed int64  // bytes of arenaLen already written to spillFile
	arenaPending []byte // interned entries not yet flushed to spillFile

	shards [numShards]urlShard

	// Stage B disk index (spec 2072 doc 04): when set, the URL location index lives
	// in an on-disk DRUM instead of the resident shards map, so the index no longer
	// costs ~80-90 B/url resident (the ~8 GiB held heap the size ladder measured at
	// 100M). The shards map stays allocated but unused for URLs in this mode; the
	// host table and arena are unchanged. mergeBatch is the unmerged-discovery count
	// that triggers a fold into the repository.
	diskIndex  bool
	drum       *drum.DRUM
	mergeBatch int64

	hostMu sync.Mutex
	hosts  map[uint64]*hostLoc

	// Larger-than-memory: residentBudget caps the number of resident URL records.
	// Zero means unbounded (the small-partition and benchmark case). Eviction is
	// oldest-materialized-first, which matches the frontier's access pattern: a
	// just-crawled URL is hot, a URL due far out will not be read until its due
	// time (doc 11 section 6.2).
	residentBudget int
	residentN      atomic.Int64
	evictMu        sync.Mutex
	evictQ         []meguri.URLKey

	id        uint32
	created   uint32
	codec     uint8
	hostKeyLo uint64
	hostKeyHi uint64
}

// Options configure a store at open.
type Options struct {
	Durability     Durability // None | Normal | Full; default Normal
	ResidentBudget int        // max resident URL records, 0 = unbounded
	PartitionID    uint32     // stamped into the .meguri snapshot
	CreatedHours   uint32     // build time, epoch-hours

	// SpillArena turns on the Stage A spilled arena (spec 2072 doc 05): the
	// canonical-URL string region lives in a disk-backed spill file read through a
	// bounded LRU instead of a fully resident []byte, removing ~70 B/url (~7 GiB at
	// 100M) from the held heap. ArenaBudget is the LRU's resident byte ceiling
	// (B_arena); 0 with SpillArena on picks a default working-set budget.
	SpillArena  bool
	ArenaBudget int64

	// DiskIndex turns on the Stage B on-disk DRUM index (spec 2072 doc 04): the URL
	// location index moves out of the resident shards map into a sorted on-disk
	// repository, removing the ~80-90 B/url resident index term. MergeBatch is the
	// number of buffered discoveries that triggers a merge into the repository; 0
	// picks a default.
	DiskIndex  bool
	MergeBatch int
}

// defaultMergeBatch is the unmerged-discovery count that triggers a DRUM merge when
// MergeBatch is left 0: 2,000,000 entries, large enough that each merge amortizes
// the repository sweep over a big batch (doc 04 section 5.1) while keeping the
// in-flight buffer and the staleness window bounded.
const defaultMergeBatch = 2_000_000

// defaultArenaBudget is the B_arena used when SpillArena is on and ArenaBudget is
// left 0: 64 MiB, the doc 52 measured sweet spot where the spill is a clear win on
// the 10M corpus (the dispatch working set is far below the full arena at scale).
const defaultArenaBudget = 64 << 20

// Open opens or recovers a partition store rooted at dir. If dir holds a valid
// checkpoint, Open loads the .meguri snapshot, rebuilds the index, and replays
// the log tail past the snapshot's frontier (doc 11 section 5). Otherwise it
// starts a fresh, empty store. Either way the returned store is ready to serve
// reads and accept updates.
func Open(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if opts.Durability == 0 {
		opts.Durability = DurabilityNormal
	}
	s := &Store{
		dir:            dir,
		dur:            opts.Durability,
		hosts:          make(map[uint64]*hostLoc),
		residentBudget: opts.ResidentBudget,
		id:             opts.PartitionID,
		created:        opts.CreatedHours,
		codec:          format.CodecZstd,
		arena:          []byte{0}, // offset-0 sentinel, the "none" reference
		hostKeyHi:      ^uint64(0),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[meguri.URLKey]*urlLoc)
	}
	if opts.DiskIndex {
		s.diskIndex = true
		s.mergeBatch = int64(opts.MergeBatch)
		if s.mergeBatch <= 0 {
			s.mergeBatch = defaultMergeBatch
		}
		d, err := drum.Open(dir, drum.Options{})
		if err != nil {
			return nil, err
		}
		s.drum = d
	}

	meta, err := readSuperblock(s.superPath())
	if err == ErrNoCheckpoint {
		// Never checkpointed: there is no snapshot, but a log may already exist from
		// a crash before the first checkpoint, so recover still replays it. A truly
		// fresh directory replays an empty log-1 and comes back empty.
		meta = checkpointMeta{gen: 0, frontier: 1, snapshot: "", logName: "log-1"}
	} else if err != nil {
		return nil, err
	}
	if err := s.recover(meta); err != nil {
		return nil, err
	}
	if opts.SpillArena {
		budget := opts.ArenaBudget
		if budget <= 0 {
			budget = defaultArenaBudget
		}
		if err := s.enableArenaSpill(budget); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// arenaPath is the spill file holding the disk-backed string region (Stage A). It
// is a derived cache, rebuilt at every Open from the recovered resident arena, so
// it carries no superblock pointer and needs no crash-consistency of its own.
func (s *Store) arenaPath() string { return filepath.Join(s.dir, "arena.bin") }

// enableArenaSpill switches the store onto the Stage A spilled arena (spec 2072
// doc 05) after recover has rebuilt the resident arena. It writes the recovered
// arena bytes to the spill file verbatim (so a byte offset into the file is the
// same arena offset the resident []byte used), opens it for positioned reads
// through a B_arena-bounded LRU, and drops the resident copy so the held heap no
// longer carries the ~70 B/url string region. Offsets are preserved exactly, so
// every URLRef a record holds resolves identically before and after the switch.
func (s *Store) enableArenaSpill(budget int64) error {
	s.arenaMu.Lock()
	defer s.arenaMu.Unlock()

	// Disk-index spill mode: arena.bin is the durable home of the string region
	// (#43), not a derived cache. Intern dropped the kindIntern frames, so the
	// recovered resident arena is empty and the file on disk is the only record of
	// the strings. Reopen it in place and take its byte length as the live arena
	// length; truncating it (the fresh path below) would erase every interned URL.
	// A zero-length or absent file means a fresh store, so fall through and create it.
	// recover leaves the resident arena at the 1-byte offset-0 sentinel when no
	// kindIntern frame was replayed (the #43 disk-index case), so sentinel-only
	// counts as empty here; a larger arena means an older log still carried the
	// frames and the create path below rebuilds arena.bin from them.
	if s.diskIndex && len(s.arena) <= 1 {
		if fi, err := os.Stat(s.arenaPath()); err == nil && fi.Size() > 0 {
			f, err := os.OpenFile(s.arenaPath(), os.O_RDWR, 0o644)
			if err != nil {
				return err
			}
			n := fi.Size()
			s.arenaLen = n
			s.arenaFlushed = n
			s.spillFile = f
			s.spill = newSpilledArena(f, n, budget)
			s.arena = nil
			return nil
		}
	}

	f, err := os.OpenFile(s.arenaPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(s.arena, 0); err != nil {
		f.Close()
		return err
	}
	s.arenaLen = int64(len(s.arena))
	s.arenaFlushed = s.arenaLen // the initial region is fully on disk
	s.spillFile = f
	s.spill = newSpilledArena(f, s.arenaLen, budget)
	s.arena = nil // free the resident region; the spill file is the source now
	return nil
}

// arenaFlushThreshold is the pending-buffer size at which the spill appender
// flushes to the file in one positioned write. Buffering turns the per-intern
// WriteAt (one blocking syscall per URL, the dominant spill ingest cost measured
// in doc 52) into one write per ~1 MiB of interned strings, roughly 15000 URLs,
// without changing durability: arena.bin is a derived cache rebuilt from the
// kindIntern log frames at Open, so an unflushed tail is recovered, not lost.
const arenaFlushThreshold = 1 << 20

// appendSpill buffers a freshly interned entry and returns the offset it was
// placed at, advancing the logical arena length and the reader's size bound. The
// bytes are flushed to the spill file in ~1 MiB blocks (flushArena), or on demand
// before a read that would touch the unflushed tail. The caller holds arenaMu, so
// the offset assignment and the buffer append are one step against any reader.
func (s *Store) appendSpill(entry []byte) (uint64, error) {
	off := uint64(s.arenaLen)
	s.arenaPending = append(s.arenaPending, entry...)
	s.arenaLen += int64(len(entry))
	s.spill.setSize(s.arenaLen)
	if len(s.arenaPending) >= arenaFlushThreshold {
		if err := s.flushArena(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// flushArena writes the pending interned bytes to the spill file at the flushed
// watermark and advances it. It is called under arenaMu when the buffer fills,
// before a read of the unflushed tail, and before the checkpoint reads the whole
// region back. A no-op when the buffer is empty.
func (s *Store) flushArena() error {
	if len(s.arenaPending) == 0 {
		return nil
	}
	if _, err := s.spillFile.WriteAt(s.arenaPending, s.arenaFlushed); err != nil {
		return err
	}
	s.arenaFlushed += int64(len(s.arenaPending))
	s.arenaPending = s.arenaPending[:0]
	return nil
}

// readArenaRegion returns the whole string region as a flat buffer for the
// checkpoint's snapshot string region, the durable home of the canonical URL
// strings (spec 2072 doc 05 section 2b). The caller holds arenaMu. In spill mode
// the resident s.arena is nil, so the region is read back from the spill file
// (the pending tail flushed first so the file holds it all); this is a
// checkpoint-time full read, not a steady-state cost, and the streaming
// checkpoint explicitly materializes the arena in its shell. Without spill the
// resident arena is copied directly.
func (s *Store) readArenaRegion() []byte {
	if s.spill == nil {
		return append([]byte(nil), s.arena...)
	}
	_ = s.flushArena()
	strs := make([]byte, s.arenaLen)
	if _, err := s.spillFile.ReadAt(strs, 0); err != nil {
		return strs[:0]
	}
	return strs
}

func (s *Store) superPath() string           { return filepath.Join(s.dir, "super") }
func (s *Store) logPath(name string) string  { return filepath.Join(s.dir, name) }
func (s *Store) snapPath(name string) string { return filepath.Join(s.dir, name) }

// shard returns the index stripe a URLKey falls in. The HostKey's low bits pick
// the shard, so all of a host's URLs share a shard, which keeps a host scan and a
// host's updates on one lock (doc 11 section 1.1).
func (s *Store) shard(k meguri.URLKey) *urlShard {
	return &s.shards[k.HostKey&(numShards-1)]
}

// Intern appends s to the string arena and returns its byte offset, the
// reference a record's *Ref field carries (doc 11 section 3.3). The append is
// logged so recovery rebuilds the arena to identical offsets; offsets are stable
// for the life of the store, so a checkpoint hands the arena to the file with no
// remap, matching the frontier's no-remap invariant. The empty string returns 0,
// the none sentinel, without growing the arena.
func (s *Store) Intern(str string) (uint64, error) {
	if str == "" {
		return 0, nil
	}
	entry := appendUvarint(nil, uint64(len(str)))
	entry = append(entry, str...)
	s.arenaMu.Lock()
	var off uint64
	if s.spill != nil {
		var err error
		if off, err = s.appendSpill(entry); err != nil {
			s.arenaMu.Unlock()
			return 0, err
		}
	} else {
		off = uint64(len(s.arena))
		s.arena = append(s.arena, entry...)
	}
	s.arenaMu.Unlock()
	// In disk-index spill mode the spill file is the durable home of the string
	// region (#43), so the kindIntern frame is dropped: it would be a second copy of
	// the bytes already in arena.bin, the single largest removable waste in the log
	// (~79 B/url, the doc 00 disk drift). Recovery reads the strings back from
	// arena.bin directly. Every other mode keeps logging the frame: resident-arena
	// stores have no spill file, and non-disk-index spill stores rotate the log at
	// each checkpoint and rebuild the post-checkpoint arena tail from it.
	if !s.internDurableInArena() {
		if _, _, err := s.log.append(kindIntern, 0, nil, entry); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// internDurableInArena reports whether interned strings are durable in the spill
// file alone, so the kindIntern log frame can be skipped. That holds only when the
// arena is spilled and the index is the DRUM (the 100M path), where the log is
// never rotated and arena.bin is reopened in place at recovery.
func (s *Store) internDurableInArena() bool { return s.spill != nil && s.diskIndex }

// Str reads back the string interned at off, the inverse of Intern. A zero or
// out-of-range offset returns the empty string, so the none sentinel and a stale
// reference both degrade to empty rather than panicking.
func (s *Store) Str(off uint64) string {
	s.arenaMu.Lock()
	defer s.arenaMu.Unlock()
	if s.spill != nil {
		if off >= uint64(s.arenaFlushed) && off < uint64(s.arenaLen) {
			if err := s.flushArena(); err != nil {
				return ""
			}
		}
		return s.spill.readArenaAt(off)
	}
	return readArena(s.arena, off)
}

// InternRobots packs a robots blob through the doc 10 section 7 robots modes and
// interns the packed bytes, returning the reference a HostRecord's RobotsRef
// carries. The packing picks the smallest of the allow-all sentinel, raw, or
// codec-compressed, so a host serving an allow-all policy never grows the arena:
// an empty blob is the allow-all case and returns 0, the none sentinel, which a
// nil-Rules reader already treats as allow-all (robots.Rules). A non-empty blob
// is stored once, deduped by the checkpoint's content dictionary when a partition
// is split or merged.
func (s *Store) InternRobots(blob []byte) (uint64, error) {
	if len(blob) == 0 {
		return 0, nil
	}
	packed := format.PackRobots(blob, s.codec)
	entry := appendUvarint(nil, uint64(len(packed)))
	entry = append(entry, packed...)
	s.arenaMu.Lock()
	var off uint64
	if s.spill != nil {
		var err error
		if off, err = s.appendSpill(entry); err != nil {
			s.arenaMu.Unlock()
			return 0, err
		}
	} else {
		off = uint64(len(s.arena))
		s.arena = append(s.arena, entry...)
	}
	s.arenaMu.Unlock()
	if !s.internDurableInArena() {
		if _, _, err := s.log.append(kindIntern, 0, nil, entry); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// Robots reads back and unpacks the robots blob at off, the inverse of
// InternRobots. A zero or out-of-range reference, the allow-all sentinel, or a
// blob that does not unpack returns nil, which a nil-Rules reader treats as
// allow-all, so a stale or corrupt reference degrades to the permissive default
// rather than panicking.
func (s *Store) Robots(off uint64) []byte {
	s.arenaMu.Lock()
	var packed []byte
	if s.spill != nil {
		if off >= uint64(s.arenaFlushed) && off < uint64(s.arenaLen) {
			if err := s.flushArena(); err != nil {
				s.arenaMu.Unlock()
				return nil
			}
		}
		packed = s.spill.readArenaBytesAt(off)
	} else {
		packed = readArenaBytes(s.arena, off)
	}
	codec := s.codec
	s.arenaMu.Unlock()
	if len(packed) == 0 {
		return nil
	}
	blob, ok := format.UnpackRobots(packed, codec, format.RobotsSizeHint)
	if !ok {
		return nil
	}
	return blob
}

// PutURL appends an updated URL record and repoints the index at it. This is the
// dominant write, the point-update-on-crawl of doc 11 section 1.1: one append,
// one index repoint, the old record left as garbage for a later compaction. It
// returns the record's LSN.
func (s *Store) PutURL(rec *meguri.URLRecord) (uint64, error) {
	var val [urlValueSize]byte
	encodeURL(val[:], rec)
	key := rec.URLKey.Bytes()
	off, lsn, err := s.log.append(kindURL, 16, key[:], val[:])
	if err != nil {
		return 0, err
	}
	if err := s.log.commit(off + int64(frameHeaderSize+16+urlValueSize+4)); err != nil {
		return 0, err
	}

	if s.diskIndex {
		// The body is durable in the log frame at off; the DRUM holds only the
		// location, folded into the repository on the next merge. No resident record,
		// no resident index slot, so the held heap does not grow per URL.
		if err := s.drum.Discover(rec.URLKey, off, lsn); err != nil {
			return 0, err
		}
		if err := s.maybeMerge(); err != nil {
			return 0, err
		}
		return lsn, nil
	}

	cp := *rec
	sh := s.shard(rec.URLKey)
	sh.mu.Lock()
	loc := sh.m[rec.URLKey]
	newResident := loc == nil || loc.rec == nil
	if loc == nil {
		loc = &urlLoc{}
		sh.m[rec.URLKey] = loc
	}
	loc.off, loc.lsn, loc.rec, loc.tomb = off, lsn, &cp, false
	sh.mu.Unlock()

	if newResident {
		s.trackResident(rec.URLKey)
	}
	return lsn, nil
}

// GetURL returns the current record for key, materializing it from disk if it was
// evicted. The bool is false if the key is absent or tombstoned.
func (s *Store) GetURL(key meguri.URLKey) (meguri.URLRecord, bool) {
	if s.diskIndex {
		off, _, present, err := s.drum.Locate(key)
		if err != nil || !present {
			return meguri.URLRecord{}, false
		}
		_, _, _, val, err := s.log.readAt(off)
		if err != nil {
			return meguri.URLRecord{}, false
		}
		return decodeURL(key, val), true
	}
	sh := s.shard(key)
	sh.mu.RLock()
	loc := sh.m[key]
	if loc == nil || loc.tomb {
		sh.mu.RUnlock()
		return meguri.URLRecord{}, false
	}
	if loc.rec != nil {
		rec := *loc.rec
		sh.mu.RUnlock()
		return rec, true
	}
	off := loc.off
	sh.mu.RUnlock()

	// Spilled read: re-materialize the record from the log (doc 11 section 6.2).
	_, _, _, val, err := s.log.readAt(off)
	if err != nil {
		return meguri.URLRecord{}, false
	}
	rec := decodeURL(key, val)
	sh.mu.Lock()
	if loc.rec == nil && !loc.tomb {
		cp := rec
		loc.rec = &cp
		sh.mu.Unlock()
		s.trackResident(key)
	} else {
		sh.mu.Unlock()
	}
	return rec, true
}

// DeleteURL tombstones a key, the rare removal of a URL from the partition (a
// host moves, doc 11 section 3.4). The everyday "this URL is dead" is a Gone
// status on a live row, not a delete.
func (s *Store) DeleteURL(key meguri.URLKey) error {
	kb := key.Bytes()
	off, lsn, err := s.log.append(kindURL|tombBit, 16, kb[:], nil)
	if err != nil {
		return err
	}
	if err := s.log.commit(off + int64(frameHeaderSize+16+4)); err != nil {
		return err
	}
	if s.diskIndex {
		if err := s.drum.Tombstone(key, off, lsn); err != nil {
			return err
		}
		return s.maybeMerge()
	}
	sh := s.shard(key)
	sh.mu.Lock()
	loc := sh.m[key]
	if loc == nil {
		loc = &urlLoc{}
		sh.m[key] = loc
	}
	loc.off, loc.lsn, loc.rec, loc.tomb = off, lsn, nil, true
	sh.mu.Unlock()
	return nil
}

// PutHost appends an updated host record and repoints the host index. A host
// update is a same-size bump in the common case (doc 11 section 3.4); the store
// logs it the same way regardless.
func (s *Store) PutHost(rec *meguri.HostRecord) (uint64, error) {
	var val [hostValueSize]byte
	encodeHost(val[:], rec)
	var key [8]byte
	putU64(key[:], rec.HostKey)
	off, lsn, err := s.log.append(kindHost, 8, key[:], val[:])
	if err != nil {
		return 0, err
	}
	if err := s.log.commit(off + int64(frameHeaderSize+8+hostValueSize+4)); err != nil {
		return 0, err
	}
	s.hostMu.Lock()
	loc := s.hosts[rec.HostKey]
	if loc == nil {
		loc = &hostLoc{}
		s.hosts[rec.HostKey] = loc
	}
	loc.off, loc.lsn, loc.rec, loc.tomb = off, lsn, *rec, false
	s.hostMu.Unlock()
	return lsn, nil
}

// GetHost returns the current record for a host key.
func (s *Store) GetHost(key uint64) (meguri.HostRecord, bool) {
	s.hostMu.Lock()
	defer s.hostMu.Unlock()
	loc := s.hosts[key]
	if loc == nil || loc.tomb {
		return meguri.HostRecord{}, false
	}
	return loc.rec, true
}

// URLCount reports the number of live (non-tombstoned) URL records.
func (s *Store) URLCount() int {
	if s.diskIndex {
		_ = s.forceMerge()
		var n int
		_ = s.drum.ScanRepo(func(meguri.URLKey, int64, uint64) error { n++; return nil })
		return n
	}
	var n int
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, loc := range sh.m {
			if !loc.tomb {
				n++
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// HostCount reports the number of live host records.
func (s *Store) HostCount() int {
	s.hostMu.Lock()
	defer s.hostMu.Unlock()
	var n int
	for _, loc := range s.hosts {
		if !loc.tomb {
			n++
		}
	}
	return n
}

// Resident reports the current number of resident URL records, the figure the
// resident budget bounds.
func (s *Store) Resident() int { return int(s.residentN.Load()) }

// LSN reports the next LSN the store will assign, the value a checkpoint records
// as its durable frontier.
func (s *Store) LSN() uint64 { return s.log.peekLSN() }

// trackResident registers a key as newly resident and evicts the oldest resident
// records if the budget is exceeded.
func (s *Store) trackResident(key meguri.URLKey) {
	if s.residentBudget <= 0 {
		return
	}
	s.residentN.Add(1)
	s.evictMu.Lock()
	s.evictQ = append(s.evictQ, key)
	for int(s.residentN.Load()) > s.residentBudget && len(s.evictQ) > 0 {
		victim := s.evictQ[0]
		s.evictQ = s.evictQ[1:]
		sh := s.shard(victim)
		sh.mu.Lock()
		if loc := sh.m[victim]; loc != nil && loc.rec != nil && !loc.tomb {
			loc.rec = nil // drop the body, keep the on-disk offset
			s.residentN.Add(-1)
		}
		sh.mu.Unlock()
	}
	s.evictMu.Unlock()
}

func putU64(b []byte, v uint64) {
	for i := range 8 {
		b[i] = byte(v >> (8 * i))
	}
}

// Close flushes the log and closes the file.
func (s *Store) Close() error {
	// Flush and sync the spilled arena tail before anything else. In disk-index
	// spill mode arena.bin is the durable home of the string region (#43), so a
	// post-checkpoint intern buffered in arenaPending must reach the file or a
	// recovered URLRef into the tail would dangle. A no-op in the other modes, where
	// arena.bin is a derived cache the log can rebuild.
	if s.spill != nil {
		s.arenaMu.Lock()
		if err := s.flushArena(); err != nil {
			s.arenaMu.Unlock()
			return err
		}
		if err := s.spillFile.Sync(); err != nil {
			s.arenaMu.Unlock()
			return err
		}
		s.arenaMu.Unlock()
	}
	if err := s.log.syncAll(); err != nil {
		return err
	}
	if s.diskIndex && s.drum != nil {
		if err := s.drum.Close(); err != nil {
			return err
		}
	}
	return s.log.close()
}

// Checkpoint folds the live store into a .meguri snapshot plus the durable log
// frontier, then swaps it in (doc 11 section 4). The snapshot is the partition's
// frontier serialized into doc 10's columnar regions; it is at once the recovery
// point, the redistribution unit, and the cold archive (D1). After the swap the
// store writes to a fresh log, so the next recovery loads this snapshot and
// replays only the updates that follow it.
func (s *Store) Checkpoint() error {
	return s.commit(s.snapshotPartition())
}

// Snapshot streams the live store into a sorted format.Partition without writing
// anything, the read-only half of a checkpoint. The top-level partition lifecycle
// (engine.OpenPartition) calls it to recover a resident frontier from a store it
// just opened, then advances that frontier and folds it back through
// CheckpointFrom.
func (s *Store) Snapshot() *format.Partition {
	return s.snapshotPartition()
}

// CheckpointFrom commits an externally built partition snapshot as the store's
// durable checkpoint, the path the top-level lifecycle uses to persist a frontier
// it recovered from this store and then advanced. It writes the snapshot, rotates
// the log, and commits the superblock exactly as Checkpoint does, but takes the
// partition from the caller (the resident frontier's serialized form) rather than
// the store's own index. The caller's frontier is the live truth for the rest of
// the session; the store's in-memory index is not re-synced here because nothing
// reads it again before the next process reopens from the committed snapshot.
func (s *Store) CheckpointFrom(part *format.Partition) error {
	return s.commit(part)
}

// commit writes part as the next durable checkpoint and rotates the log. It is the
// shared body of Checkpoint (snapshot sourced from the store's own index) and
// CheckpointFrom (snapshot sourced from a caller's frontier).
func (s *Store) commit(part *format.Partition) error {
	// Stream the snapshot to disk one region at a time rather than materializing
	// the whole image and writing it in a second pass: at 100M urls the doubled
	// in-memory image is several GB of avoidable checkpoint transient on top of the
	// record slice the snapshot already pays (scale doc 12, F4). EncodeToFile
	// produces byte-identical output, pinned by TestEncodeToFileMatchesEncode.
	return s.commitSnap(len(part.Strings), func(path string) error {
		return format.EncodeToFile(path, part)
	})
}

// commitSnap is the durable half every checkpoint shares: it writes the next
// snapshot file (via writeSnap, which the caller picks: the materializing
// EncodeToFile, or the bounded StreamEncodeToFile), rotates the log so
// post-checkpoint updates continue the LSN sequence in a fresh log, and commits
// the two-slot superblock only after the snapshot is durable. arenaLen is the
// string region length the superblock records for recovery.
func (s *Store) commitSnap(arenaLen int, writeSnap func(path string) error) error {
	// The consistent cut: the frontier LSN the snapshot is consistent as of is the
	// store's next LSN, captured before the rotation. The simplest correct cut for
	// a frontier that can briefly pause its own dispatch is to checkpoint between
	// updates; concurrent writers see the snapshot at this LSN (doc 11 section 4.2).
	frontier := s.log.peekLSN()

	nextGen := s.gen + 1
	snapName := fmt.Sprintf("snap-%d.meguri", nextGen)
	if err := writeSnap(s.snapPath(snapName)); err != nil {
		return err
	}

	if s.diskIndex {
		// Disk-index mode: the DRUM repository is the durable index and the append-only
		// log is the durable body store its offsets address, so the log is not rotated
		// or reclaimed at a checkpoint. Rotating it would strand every offset the DRUM
		// holds for a record that was not re-logged after the cut. The .meguri snapshot
		// is written as a portable export (redistribution, cold archive), not as the
		// local home of the bodies. Compacting the body log, or paging bodies out of the
		// columnar snapshot so the log can be reclaimed, is the doc 14 follow-up; until
		// then the body log grows with the corpus, which is the on-disk body term the
		// size ladder already budgets, not a resident cost.
		meta := checkpointMeta{
			gen:      nextGen,
			frontier: frontier,
			arenaLen: uint64(arenaLen),
			snapshot: snapName,
			logName:  s.logName,
		}
		if err := writeSuperblock(s.superPath(), meta); err != nil {
			return err
		}
		prevGen := s.gen
		s.gen = nextGen
		if prevGen > 0 {
			_ = os.Remove(s.snapPath(fmt.Sprintf("snap-%d.meguri", prevGen)))
		}
		return nil
	}

	// Rotate to a fresh log that continues the LSN sequence, so post-checkpoint
	// updates land in the new log and the old one becomes reclaimable.
	newLogName := fmt.Sprintf("log-%d", nextGen+1)
	newLog, err := openLog(s.logPath(newLogName), s.dur)
	if err != nil {
		return err
	}
	newLog.setLSN(frontier)

	// Commit: record the new snapshot and log only after the snapshot is durable
	// (the two-slot superblock, section 4.2 steps 6-7). A crash before this fsync
	// recovers from the old slot; a crash after recovers from the new one.
	meta := checkpointMeta{
		gen:      nextGen,
		frontier: frontier,
		arenaLen: uint64(arenaLen),
		snapshot: snapName,
		logName:  newLogName,
	}
	if err := writeSuperblock(s.superPath(), meta); err != nil {
		_ = newLog.close()
		return err
	}

	old := s.log
	s.log = newLog
	s.logName = newLogName
	s.gen = nextGen
	_ = old.close()

	// Reclaim the superseded log and snapshot; their loss is harmless because the
	// new checkpoint is the durable home of everything they held.
	_ = os.Remove(s.logPath(fmt.Sprintf("log-%d", nextGen)))
	if nextGen > 1 {
		_ = os.Remove(s.snapPath(fmt.Sprintf("snap-%d.meguri", s.gen-1)))
	}
	return nil
}

// snapshotPartition streams every live record, resident or spilled, into a
// sorted format.Partition (doc 11 section 4.2 steps 2-4). Reading each record
// through the index means a larger-than-memory store checkpoints by streaming,
// not by holding the whole frontier resident.
func (s *Store) snapshotPartition() *format.Partition {
	var urls []meguri.URLRecord
	if s.diskIndex {
		// The repository is already globally sorted by URLKey, the snapshot's row
		// order, so a scan yields the records in order with no extra sort. Bodies are
		// read from the log at the location the DRUM stored.
		_ = s.forceMerge()
		_ = s.drum.ScanRepo(func(key meguri.URLKey, off int64, _ uint64) error {
			if _, _, _, val, err := s.log.readAt(off); err == nil {
				urls = append(urls, decodeURL(key, val))
			}
			return nil
		})
	} else {
		urls = make([]meguri.URLRecord, 0, s.URLCount())
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.RLock()
			for key, loc := range sh.m {
				if loc.tomb {
					continue
				}
				if loc.rec != nil {
					urls = append(urls, *loc.rec)
					continue
				}
				off := loc.off
				sh.mu.RUnlock()
				if _, _, _, val, err := s.log.readAt(off); err == nil {
					urls = append(urls, decodeURL(key, val))
				}
				sh.mu.RLock()
			}
			sh.mu.RUnlock()
		}
		sort.Slice(urls, func(i, j int) bool { return urls[i].URLKey.Less(urls[j].URLKey) })
	}

	s.hostMu.Lock()
	hosts := make([]meguri.HostRecord, 0, len(s.hosts))
	for _, loc := range s.hosts {
		if !loc.tomb {
			hosts = append(hosts, loc.rec)
		}
	}
	s.hostMu.Unlock()
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostKey < hosts[j].HostKey })

	lo, hi := uint64(0), ^uint64(0)
	if len(hosts) > 0 {
		lo, hi = hosts[0].HostKey, hosts[len(hosts)-1].HostKey
	}
	s.arenaMu.Lock()
	strs := s.readArenaRegion()
	s.arenaMu.Unlock()
	return &format.Partition{
		ID:           s.id,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: s.created,
		DefaultCodec: s.codec,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      strs,
	}
}

// recover rebuilds the store from a committed checkpoint: load the snapshot,
// install the index, then replay the log tail past the snapshot's frontier (doc
// 11 section 5.1). It is redo-only, idempotent by LSN, and stops at the first
// torn frame.
func (s *Store) recover(meta checkpointMeta) error {
	s.gen = meta.gen
	s.logName = meta.logName

	if meta.snapshot != "" {
		img, err := os.ReadFile(s.snapPath(meta.snapshot))
		if err != nil {
			return err
		}
		part, err := format.Decode(img)
		if err != nil {
			return err
		}
		s.id = part.ID
		s.created = part.CreatedHours
		s.codec = part.DefaultCodec
		s.hostKeyLo, s.hostKeyHi = part.HostKeyLo, part.HostKeyHi
		// In disk-index mode the log is never rotated (commitSnap keeps it as the body
		// store), so it holds every intern frame since gen 0 and the replay below
		// rebuilds the arena at its exact original offsets, starting from the same
		// offset-0 sentinel. Loading the snapshot Strings too would prepend a full
		// second copy, so a post-checkpoint re-intern (Intern never dedups) whose offset
		// is L would resolve into the duplicated region instead of its own string. The
		// snapshot is an export here, not the arena's home, so its Strings are not read.
		if len(part.Strings) > 0 && !s.diskIndex {
			s.arena = append([]byte(nil), part.Strings...)
		}
		// Snapshot rows load resident and pinned: their bodies have no log offset
		// (lsn 0), so they are not eviction candidates and stay in memory until an
		// update moves them to the log or the next checkpoint reclaims them. The
		// resident budget bounds the live log-backed working set, the steady-state
		// churn of records pulled in, updated, and spilled (doc 11 section 6.3). A
		// per-row random read of the columnar snapshot, which would let the cold base
		// spill too, is the doc 14 follow-up.
		//
		// In disk-index mode the URL index of record is the DRUM repository, which is
		// durable on disk and reopened intact, so the snapshot rows are not loaded into
		// any resident map and not re-fed to the DRUM: the .meguri snapshot is a
		// projection of the repository (snapshotPartition streams it from ScanRepo), so
		// the repository already holds every snapshot key. Migrating a non-disk-index
		// snapshot into the DRUM is the doc 09 path and is not done here; disk-index
		// stores are grown disk-index from the start.
		if !s.diskIndex {
			for i := range part.URLs {
				rec := part.URLs[i]
				s.shard(rec.URLKey).m[rec.URLKey] = &urlLoc{rec: &rec, lsn: 0}
			}
		}
		for i := range part.Hosts {
			h := part.Hosts[i]
			s.hosts[h.HostKey] = &hostLoc{rec: h}
		}
	}

	l, err := openLog(s.logPath(meta.logName), s.dur)
	if err != nil {
		return err
	}
	s.log = l

	var maxLSN uint64
	err = l.replay(func(fr frame) error {
		if fr.lsn > maxLSN {
			maxLSN = fr.lsn
		}
		switch fr.kind {
		case kindIntern:
			s.arena = append(s.arena, fr.val...)
		case kindURL:
			key := meguri.URLKeyFromBytes([16]byte(fr.key))
			if s.diskIndex {
				// The log frame carries the body's offset and lsn the DRUM indexes by,
				// so replay folds the post-merge tail back into the DRUM. The tail is
				// the writes since the last merge whose in-memory buckets were lost on
				// the crash; re-feeding already-merged keys is idempotent (the merge
				// dedups by key and keeps the highest lsn). A final merge after replay
				// makes them durable in the repository again.
				if fr.tomb {
					return s.drum.Tombstone(key, fr.off, fr.lsn)
				}
				return s.drum.Insert(key, fr.off, fr.lsn)
			}
			sh := s.shard(key)
			loc := sh.m[key]
			if loc == nil {
				loc = &urlLoc{}
				sh.m[key] = loc
			}
			if fr.tomb {
				loc.off, loc.lsn, loc.rec, loc.tomb = fr.off, fr.lsn, nil, true
			} else {
				rec := decodeURL(key, fr.val)
				loc.off, loc.lsn, loc.rec, loc.tomb = fr.off, fr.lsn, &rec, false
			}
		case kindHost:
			key := readU64(fr.key)
			loc := s.hosts[key]
			if loc == nil {
				loc = &hostLoc{}
				s.hosts[key] = loc
			}
			if fr.tomb {
				loc.tomb = true
			} else {
				loc.rec = decodeHost(key, fr.val)
				loc.tomb = false
			}
			loc.off, loc.lsn = fr.off, fr.lsn
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Fold the replayed tail into the repository so a point read right after Open is
	// served from disk, not the overlay, and the next checkpoint streams a complete
	// repository.
	if s.diskIndex {
		if err := s.forceMerge(); err != nil {
			return err
		}
	}

	// Resume past the highest durable LSN so the next write cannot collide with a
	// recovered one (doc 11 section 5.1 step 6).
	l.setLSN(max(meta.frontier, maxLSN+1))
	return nil
}

func readU64(b []byte) uint64 {
	var v uint64
	for i := range 8 {
		v |= uint64(b[i]) << (8 * i)
	}
	return v
}
