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

	shards [numShards]urlShard

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
}

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
	return s, nil
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
	s.arenaMu.Lock()
	off := uint64(len(s.arena))
	entry := appendUvarint(nil, uint64(len(str)))
	entry = append(entry, str...)
	s.arena = append(s.arena, entry...)
	s.arenaMu.Unlock()
	if _, _, err := s.log.append(kindIntern, 0, nil, entry); err != nil {
		return 0, err
	}
	return off, nil
}

// Str reads back the string interned at off, the inverse of Intern. A zero or
// out-of-range offset returns the empty string, so the none sentinel and a stale
// reference both degrade to empty rather than panicking.
func (s *Store) Str(off uint64) string {
	s.arenaMu.Lock()
	defer s.arenaMu.Unlock()
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
	s.arenaMu.Lock()
	off := uint64(len(s.arena))
	entry := appendUvarint(nil, uint64(len(packed)))
	entry = append(entry, packed...)
	s.arena = append(s.arena, entry...)
	s.arenaMu.Unlock()
	if _, _, err := s.log.append(kindIntern, 0, nil, entry); err != nil {
		return 0, err
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
	packed := readArenaBytes(s.arena, off)
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
	if err := s.log.syncAll(); err != nil {
		return err
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
	// The consistent cut: the frontier LSN the snapshot is consistent as of is the
	// store's next LSN, captured before the rotation. The simplest correct cut for
	// a frontier that can briefly pause its own dispatch is to checkpoint between
	// updates; concurrent writers see the snapshot at this LSN (doc 11 section 4.2).
	frontier := s.log.peekLSN()

	nextGen := s.gen + 1
	snapName := fmt.Sprintf("snap-%d.meguri", nextGen)
	img, err := format.Encode(part)
	if err != nil {
		return err
	}
	if err := writeFileSync(s.snapPath(snapName), img); err != nil {
		return err
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
		arenaLen: uint64(len(part.Strings)),
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
	urls := make([]meguri.URLRecord, 0, s.URLCount())
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
	strs := append([]byte(nil), s.arena...)
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
		if len(part.Strings) > 0 {
			s.arena = append([]byte(nil), part.Strings...)
		}
		// Snapshot rows load resident and pinned: their bodies have no log offset
		// (lsn 0), so they are not eviction candidates and stay in memory until an
		// update moves them to the log or the next checkpoint reclaims them. The
		// resident budget bounds the live log-backed working set, the steady-state
		// churn of records pulled in, updated, and spilled (doc 11 section 6.3). A
		// per-row random read of the columnar snapshot, which would let the cold base
		// spill too, is the doc 14 follow-up.
		for i := range part.URLs {
			rec := part.URLs[i]
			s.shard(rec.URLKey).m[rec.URLKey] = &urlLoc{rec: &rec, lsn: 0}
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
