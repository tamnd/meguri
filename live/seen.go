package live

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// seenBuilder collects the distinct URL keys a seal freezes into the seen-set
// filter region. The blocked-Bloom form (dedup.Filter) added keys one at a time as
// the build streamed them, so a seal could size an empty filter up front and Add
// as it went. The ribbon (dedup/ribbon.go) is a static structure solved once over
// the whole key set by Gaussian elimination, so it cannot be fed incrementally: the
// seal collects the keys and builds the filter at Marshal.
//
// The keys arrive in URLKey order on every seal path (the bulk phase-2 merge and the
// compaction merge-join both emit sorted), so a duplicate is always adjacent to its
// twin and is dropped in place. The ribbon needs each key exactly once or its linear
// system is over-determined and the solve fails.
//
// A large seal partitions its keys into shards so the solve at Marshal runs one small
// ribbon per shard in parallel (dedup/ribbon_sharded.go) instead of one linear system
// over the whole set. A key routes to its shard by dedup.RibbonShardIndex, and since
// equal keys hash alike they land in the same shard adjacent to each other, so the
// in-place duplicate drop still works per shard. The shard count comes from the seal's
// size hint, which every seal path knows, and the filter blob records it so the query
// path routes identically.
//
// The keys are the seal's largest transient: at 100M they are 1.6 GiB of URLKey, the
// term that pushed the compaction peak over a small box's memory. When the shard files
// fit the open-file budget the collector spills each shard's keys to a temp file as they
// arrive (shardSpill) and solves each shard by reading its file back, so the resident
// set is a handful of shards being solved rather than the whole key slice. Below the
// shard threshold, and when the spill will not fit the file budget, it keeps the keys in
// memory; a single-shard seal is byte-for-byte the old single ribbon.
type seenBuilder struct {
	r      int
	shards [][]m.URLKey // in-memory mode: per-shard key slices (nil when spilling)
	spill  *shardSpill  // disk mode: per-shard spill files (nil when in memory)
}

// newSeenBuilder sizes an empty collector. hint is the expected distinct key count: it
// picks the shard count (dedup.RibbonShardCount) and, when it keeps the keys in memory,
// pre-grows each shard slice to its share of the hint so a large seal does not
// repeatedly reallocate. A multi-shard seal spills to files under tmpDir when the shard
// count fits the process open-file budget, keeping the 100M key set off the heap. The
// fpRate picks the ribbon fingerprint width r, the same one-sided false-positive knob
// the blocked-Bloom took (dedup.RibbonBitsForFPR).
func newSeenBuilder(fpRate float64, hint uint64, tmpDir string) (*seenBuilder, error) {
	r := dedup.RibbonBitsForFPR(fpRate)
	shardCount := dedup.RibbonShardCount(int(hint))

	// Spill to disk only when there is more than one shard and the files fit the budget;
	// otherwise hold the keys in memory. The budget guard keeps the spill from failing on
	// a platform with a tight open-file limit, falling back to the in-memory shards that
	// answer identically.
	if shardCount > 1 && shardCount+spillFileMargin <= openFileBudget() {
		sp, err := newShardSpill(tmpDir, shardCount)
		if err != nil {
			return nil, err
		}
		return &seenBuilder{r: r, spill: sp}, nil
	}

	shards := make([][]m.URLKey, shardCount)
	// Per-shard headroom over the mean so ordinary Poisson spread across shards does
	// not trigger a synchronized reallocation as they all fill together.
	per := int(hint)/shardCount + int(hint)/(shardCount*16) + 64
	for i := range shards {
		shards[i] = make([]m.URLKey, 0, per)
	}
	return &seenBuilder{r: r, shards: shards}, nil
}

// addSorted records a key that arrives in nondecreasing URLKey order, routing it to
// its shard and dropping an exact repeat of that shard's last key. Equal keys hash to
// the same shard and arrive adjacent, so the per-shard tail check drops the duplicate
// the ribbon solve cannot take twice. This is the collection point on the seal merge
// loops, where the rows are already key-ordered.
func (s *seenBuilder) addSorted(key m.URLKey) error {
	if s.spill != nil {
		return s.spill.add(key)
	}
	idx := dedup.RibbonShardIndex(key, len(s.shards))
	sh := s.shards[idx]
	if n := len(sh); n > 0 && sh[n-1] == key {
		return nil
	}
	s.shards[idx] = append(sh, key)
	return nil
}

// marshal solves the ribbon over the collected keys and returns the seen-set region
// blob plus its realized bits per key, the residency-gate number the build reports.
// With one shard this is the single kind-1 ribbon; with more it is the kind-2 sharded
// ribbon solved in parallel, from the spill files when spilling and from the in-memory
// shards otherwise. An empty key set marshals to an empty ribbon that answers every
// probe false.
func (s *seenBuilder) marshal() ([]byte, float64, error) {
	var (
		blob []byte
		err  error
	)
	if s.spill != nil {
		blob, err = s.spill.build(s.r)
	} else {
		blob, err = dedup.BuildShardedRibbonFilter(s.shards, dedup.WithRibbonBits(s.r))
	}
	if err != nil {
		return nil, 0, err
	}
	rf, err := dedup.UnmarshalFilter(blob)
	if err != nil {
		return nil, 0, err
	}
	return blob, rf.BitsPerURL(), nil
}

// spillFileMargin reserves open-file headroom for the base mmap, arena, records temp,
// merge runs, and the standard streams alongside the per-shard spill files.
const spillFileMargin = 64

// spillWriteBuf is the per-shard write buffer. Small so many open shards cost little
// memory during collection: at 4096 shards this is 64 MiB of buffers, dwarfed by the
// key set it replaces.
const spillWriteBuf = 1 << 14 // 16 KiB

// shardSpill holds the distinct keys of a multi-shard seal in one temp file per shard.
// A key is routed by dedup.RibbonShardIndex and appended to its shard file in the
// URLKey order it arrives, so equal keys land adjacent and the per-shard tail check
// drops them the same way the in-memory collector does. At Marshal each shard file is
// read back into a key slice, solved, and freed, so only the shards in flight are
// resident. The files live under the seal's temp dir and go away with it.
type shardSpill struct {
	dir     string
	files   []*os.File
	ws      []*bufio.Writer
	last    []m.URLKey
	has     []bool
	counts  []int64
	scratch [16]byte
}

// newShardSpill creates one temp file per shard under a fresh subdir of tmpDir.
func newShardSpill(tmpDir string, shardCount int) (*shardSpill, error) {
	dir, err := os.MkdirTemp(tmpDir, "meguri-seen-")
	if err != nil {
		return nil, err
	}
	sp := &shardSpill{
		dir:    dir,
		files:  make([]*os.File, shardCount),
		ws:     make([]*bufio.Writer, shardCount),
		last:   make([]m.URLKey, shardCount),
		has:    make([]bool, shardCount),
		counts: make([]int64, shardCount),
	}
	for i := range sp.files {
		f, err := os.Create(filepath.Join(dir, "shard-"+strconv.Itoa(i)))
		if err != nil {
			sp.closeFiles()
			_ = os.RemoveAll(dir)
			return nil, err
		}
		sp.files[i] = f
		sp.ws[i] = bufio.NewWriterSize(f, spillWriteBuf)
	}
	return sp, nil
}

// add routes a key to its shard and appends it unless it repeats that shard's last key.
func (sp *shardSpill) add(key m.URLKey) error {
	idx := dedup.RibbonShardIndex(key, len(sp.files))
	if sp.has[idx] && sp.last[idx] == key {
		return nil
	}
	binary.LittleEndian.PutUint64(sp.scratch[0:8], key.HostKey)
	binary.LittleEndian.PutUint64(sp.scratch[8:16], key.PathKey)
	if _, err := sp.ws[idx].Write(sp.scratch[:]); err != nil {
		return err
	}
	sp.last[idx] = key
	sp.has[idx] = true
	sp.counts[idx]++
	return nil
}

// build flushes and closes the write side, then solves the sharded ribbon reading each
// shard back through loadShard, so at most one shard per worker is resident during the
// solve. The spill dir is removed on the way out.
func (sp *shardSpill) build(r int) ([]byte, error) {
	defer os.RemoveAll(sp.dir)
	for i := range sp.ws {
		if sp.ws[i] != nil {
			if err := sp.ws[i].Flush(); err != nil {
				sp.closeFiles()
				return nil, err
			}
		}
		if sp.files[i] != nil {
			if err := sp.files[i].Close(); err != nil {
				return nil, err
			}
			sp.files[i] = nil
		}
	}
	var n uint64
	for _, c := range sp.counts {
		n += uint64(c)
	}
	return dedup.BuildShardedRibbonFilterDisk(len(sp.counts), n, sp.loadShard, dedup.WithRibbonBits(r))
}

// loadShard reads shard i's keys back from its file. Workers pull distinct shard indexes,
// so only as many files as workers are open at once.
func (sp *shardSpill) loadShard(i int) ([]m.URLKey, error) {
	f, err := os.Open(filepath.Join(sp.dir, "shard-"+strconv.Itoa(i)))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	keys := make([]m.URLKey, sp.counts[i])
	br := bufio.NewReaderSize(f, 1<<16)
	var buf [16]byte
	for j := range keys {
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return nil, err
		}
		keys[j] = m.URLKey{
			HostKey: binary.LittleEndian.Uint64(buf[0:8]),
			PathKey: binary.LittleEndian.Uint64(buf[8:16]),
		}
	}
	return keys, nil
}

// closeFiles closes any open shard files, used on a partial-open failure.
func (sp *shardSpill) closeFiles() {
	for i := range sp.files {
		if sp.files[i] != nil {
			_ = sp.files[i].Close()
			sp.files[i] = nil
		}
	}
}
