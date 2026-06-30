// Package drum is the on-disk seen-set and URL index of a partition (spec 2072
// doc 04, Stage B): a real Disk Repository with Update Management (IRLbot, Lee et
// al. 2008) that absorbs the dedup exact set and the store's resident URL index
// into one on-disk authority whose aux value is the record location (D2). A
// membership probe and a location lookup are the same probe, the seen-set and the
// index are maintained once on disk instead of twice resident, and the resident
// cost is a fixed budget (the in-flight bucket buffers plus a ~0.17 B/url block
// index) independent of how many distinct URLs the partition has ever seen.
//
// The shape: K=256 in-memory buckets aligned to the store's 256 shards accumulate
// discovered (URLKey, locEntry) pairs; a bucket flushes to a bucket-NNN.pending
// file when it fills; a merge k-way-merges the pending runs against the single
// sorted repository in one sequential sweep, classifying each key unique-or-
// duplicate (the dedup check) and updating its location in place (the index
// update). The point read for a cold due URL binary-searches a resident sparse
// block index to one block and reads it. Pure Go, CGO_ENABLED=0, no internal/.
package drum

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tamnd/meguri"
)

// numBuckets is K, the DRUM bucket count, locked to the store's 256 shards and the
// HostKey partition (doc 04 section 2.1): the bucket index is key.HostKey&255, the
// same low 8 bits the store shards by, so a host's keys land in one bucket, one
// shard, and one repository range with no re-hashing.
const numBuckets = 256

// Options configure a DRUM at open.
type Options struct {
	// FlushBytes is the per-bucket serialized size at which a bucket flushes to a
	// .pending file. Zero picks a 1 MiB default (~33k entries per bucket).
	FlushBytes int
	// ReadBuf and WriteBuf size the merge's sequential repository reader and writer.
	// Zero picks 1 MiB each.
	ReadBuf  int
	WriteBuf int
}

const (
	defaultFlushBytes = 1 << 20
	defaultMergeBuf   = 1 << 20
)

// bucket is one of the 256 in-memory DRUM buffers (doc 04 section 2.2). It
// accumulates entries until it reaches the flush threshold, then flushes to a
// bucket-NNN.pending file and resets. The buffer is unsorted while it accumulates
// (an append is O(1)); the sort is paid once at flush.
type bucket struct {
	mu    sync.Mutex
	buf   []pendEntry
	bytes int
	seq   uint32
}

// DRUM is a partition's on-disk seen-set and index.
type DRUM struct {
	dir      string
	drumDir  string
	repoPath string
	nextPath string
	idxPath  string

	flushBytes int
	readBuf    int
	writeBuf   int

	buckets [numBuckets]bucket

	// biMu guards the resident block index and the repository file handle, both
	// swapped atomically when a merge renames a fresh repository in.
	biMu     sync.RWMutex
	bi       *blockIndex
	repoFile *os.File

	// unmerged counts discoveries routed since the last merge (buffered in buckets
	// or flushed to pending files). When zero, a point read skips the overlay scan
	// entirely and answers straight from the repository block, the steady state for
	// a due URL (no readdir, no bucket walk).
	unmerged atomic.Int64
}

// Open opens or recovers a DRUM rooted at dir. It creates the drum/ subdirectory,
// discards any torn .pending file left by an interrupted flush (recovery re-derives
// its entries from the log, section 3.6), and loads the resident block index over
// the existing repository (rebuilding it from the repository if the index file is
// missing or torn).
func Open(dir string, opts Options) (*DRUM, error) {
	drumDir := filepath.Join(dir, "drum")
	if err := os.MkdirAll(drumDir, 0o755); err != nil {
		return nil, err
	}
	d := &DRUM{
		dir:        dir,
		drumDir:    drumDir,
		repoPath:   filepath.Join(drumDir, repoName),
		nextPath:   filepath.Join(drumDir, repoNextName),
		idxPath:    filepath.Join(drumDir, repoIdxName),
		flushBytes: opts.FlushBytes,
		readBuf:    opts.ReadBuf,
		writeBuf:   opts.WriteBuf,
	}
	if d.flushBytes <= 0 {
		d.flushBytes = defaultFlushBytes
	}
	if d.readBuf <= 0 {
		d.readBuf = defaultMergeBuf
	}
	if d.writeBuf <= 0 {
		d.writeBuf = defaultMergeBuf
	}
	// A leftover repository.next is a merge that crashed before its rename; the live
	// repository is still the old one, so the partial next is garbage.
	_ = os.Remove(d.nextPath)
	if err := d.dropTornPending(); err != nil {
		return nil, err
	}
	bi, err := loadBlockIndex(d.idxPath, d.repoPath)
	if err != nil {
		return nil, err
	}
	d.bi = bi
	if f, err := os.Open(d.repoPath); err == nil {
		d.repoFile = f
	}
	return d, nil
}

// dropTornPending discards any .pending file whose CRC does not verify, the torn
// flush an interrupted write leaves (section 3.3). A torn file's entries are
// reconstructable from the log, so discarding it costs a re-derivation, never data.
func (d *DRUM) dropTornPending() error {
	names, err := listPending(d.drumDir)
	if err != nil {
		return err
	}
	for _, p := range names {
		if _, ok, err := readPendingFile(p); err != nil {
			return err
		} else if !ok {
			if err := os.Remove(p); err != nil {
				return err
			}
		}
	}
	return nil
}

// Discover buffers a check-and-insert: the discovery wants to know whether key is
// new (the verdict comes from the next Merge) and to index its record at loc. The
// pair is routed to bucket key.HostKey&255 and the bucket flushes if it fills.
func (d *DRUM) Discover(key meguri.URLKey, off int64, lsn uint64) error {
	return d.route(pendEntry{key: key, loc: locEntry{off: off, lsn: lsn}, op: opCheckInsert})
}

// Insert buffers an insert-only entry, the recovery and handoff path: index the
// record without producing a dedup verdict (section 2.2).
func (d *DRUM) Insert(key meguri.URLKey, off int64, lsn uint64) error {
	return d.route(pendEntry{key: key, loc: locEntry{off: off, lsn: lsn}, op: opInsertOnly})
}

// Tombstone buffers a tombstone update: the key left the partition (a host moved).
// The merge carries the tomb bit into the repository by last-writer-wins like any
// other location update.
func (d *DRUM) Tombstone(key meguri.URLKey, off int64, lsn uint64) error {
	return d.route(pendEntry{key: key, loc: locEntry{off: off, lsn: lsn, tomb: true}, op: opInsertOnly})
}

func (d *DRUM) route(e pendEntry) error {
	b := &d.buckets[e.key.HostKey&(numBuckets-1)]
	b.mu.Lock()
	b.buf = append(b.buf, e)
	b.bytes += pendRecordSize
	full := b.bytes >= d.flushBytes
	b.mu.Unlock()
	d.unmerged.Add(1)
	if full {
		return d.flushBucket(int(e.key.HostKey & (numBuckets - 1)))
	}
	return nil
}

// flushBucket sorts a bucket's buffer by URLKey (check-insert before insert-only at
// a tie) and writes it to a new bucket-NNN.pending file, then resets the buffer.
// The sort here is the only sort the buffer pays; the merge then sweeps the sorted
// pending file against the sorted repository in one linear pass.
func (d *DRUM) flushBucket(idx int) error {
	b := &d.buckets[idx]
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	entries := b.buf
	seq := b.seq
	b.buf = nil
	b.bytes = 0
	b.seq++
	b.mu.Unlock()

	sortPending(entries)
	return writePendingFile(filepath.Join(d.drumDir, pendingName(idx, seq)), idx, entries)
}

func sortPending(entries []pendEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].key == entries[j].key {
			return entries[i].op < entries[j].op
		}
		return entries[i].key.Less(entries[j].key)
	})
}

// Merge folds every buffered and flushed discovery into the repository in one
// sequential sweep, returning the dedup verdicts. It flushes all non-empty buckets
// first so the merge sees a consistent cut of the discovery stream, gathers all
// .pending runs, runs the merge cycle, swaps the fresh repository and block index
// in, and drops the consumed pending files.
func (d *DRUM) Merge() ([]Classification, error) {
	for i := range d.buckets {
		if err := d.flushBucket(i); err != nil {
			return nil, err
		}
	}
	names, err := listPending(d.drumDir)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil // nothing discovered since the last merge
	}
	runs := make([]*pendRun, 0, len(names))
	for _, p := range names {
		entries, ok, err := readPendingFile(p)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // torn; its entries are re-derivable from the log
		}
		runs = append(runs, newPendRun(entries))
	}

	verdicts, bi, err := mergeCycle(d.repoPath, d.nextPath, d.idxPath, runs, d.readBuf, d.writeBuf)
	if err != nil {
		return nil, err
	}

	d.biMu.Lock()
	if d.repoFile != nil {
		_ = d.repoFile.Close()
	}
	d.bi = bi
	d.repoFile, _ = os.Open(d.repoPath)
	d.biMu.Unlock()

	for _, p := range names {
		_ = os.Remove(p)
	}
	d.unmerged.Store(0)
	return verdicts, nil
}

// Locate answers the point read: given a URLKey, return its current locEntry, or
// report absent. It consults the in-flight overlay (resident buckets and unmerged
// .pending files, which hold discoveries newer than the repository) before the
// repository, and returns the highest-LSN location across all of them so a
// rediscovery whose record moved since the last merge is found at its newest frame
// (section 4.2). A tombstoned key reports absent.
func (d *DRUM) Locate(key meguri.URLKey) (off int64, lsn uint64, present bool, err error) {
	var best locEntry
	var found bool
	if d.unmerged.Load() > 0 {
		best, found = d.overlayLocate(key)
	}

	d.biMu.RLock()
	bi, rf := d.bi, d.repoFile
	d.biMu.RUnlock()
	if rf != nil && bi != nil {
		if loc, ok, e := locateRepo(bi, rf, key); e != nil {
			return 0, 0, false, e
		} else if ok && (!found || loc.lsn >= best.lsn) {
			best, found = loc, true
		}
	}
	if !found || best.tomb {
		return 0, 0, false, nil
	}
	return best.off, best.lsn, true, nil
}

// overlayLocate scans the resident buckets and unmerged .pending files for key,
// returning the highest-LSN match. The overlay is bounded by the flush threshold
// and the merge cadence, so it is a small RAM-and-recent-file scan that almost
// always misses for a due URL (a due URL was scheduled long ago, not discovered
// seconds back, section 4.2).
func (d *DRUM) overlayLocate(key meguri.URLKey) (locEntry, bool) {
	var best locEntry
	found := false
	b := &d.buckets[key.HostKey&(numBuckets-1)]
	b.mu.Lock()
	for _, e := range b.buf {
		if e.key == key && (!found || e.loc.lsn >= best.lsn) {
			best, found = e.loc, true
		}
	}
	b.mu.Unlock()

	names, err := listPending(d.drumDir)
	if err != nil {
		return best, found
	}
	for _, p := range names {
		entries, ok, err := readPendingFile(p)
		if err != nil || !ok {
			continue
		}
		i := sort.Search(len(entries), func(i int) bool { return !entries[i].key.Less(key) })
		for ; i < len(entries) && entries[i].key == key; i++ {
			if !found || entries[i].loc.lsn >= best.lsn {
				best, found = entries[i].loc, true
			}
		}
	}
	return best, found
}

// locateRepo reads the one repository block the sparse index points at and binary-
// searches it for key (section 4.2): one ReadAt, then an in-RAM search. A key below
// the first block, or absent from its block, reports not found.
func locateRepo(bi *blockIndex, rf *os.File, key meguri.URLKey) (locEntry, bool, error) {
	off := bi.blockFor(key)
	if off < 0 {
		return locEntry{}, false, nil
	}
	buf := make([]byte, blockRecords*repoRecordSize)
	n, err := rf.ReadAt(buf, off)
	if err != nil && n < repoRecordSize {
		// A short final block is expected; only a real read error past the first
		// record is fatal.
		if n == 0 {
			return locEntry{}, false, nil
		}
	}
	recs := n / repoRecordSize
	lo, hi := 0, recs
	for lo < hi {
		mid := (lo + hi) / 2
		e := getRepoRecord(buf[mid*repoRecordSize:])
		switch e.key.Compare(key) {
		case 0:
			return e.loc, true, nil
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return locEntry{}, false, nil
}

// Close flushes nothing implicitly (a caller checkpoints by calling Merge) and
// releases the repository file handle. Buffered, unflushed discoveries are
// re-derivable from the log, so closing without a final Merge loses no data.
func (d *DRUM) Close() error {
	d.biMu.Lock()
	defer d.biMu.Unlock()
	if d.repoFile != nil {
		err := d.repoFile.Close()
		d.repoFile = nil
		return err
	}
	return nil
}
