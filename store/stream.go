package store

import (
	"container/heap"
	"sort"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// CheckpointStreaming folds the live store into the durable .meguri snapshot the
// bounded-memory way (spec 2072 D9, 2071 implementation doc 51): instead of
// materializing the whole partition as a record slice and a column-major copy
// (snapshotPartition + EncodeToFile, ~0.9 KB per row of transient that OOMs a 64 GB
// box at 100M), it streams the URL table through a k-way merge over the 256 sorted
// shards into format.StreamEncodeToFile, which spills the columns page by page. The
// resident transient is the shard key copies (a 16 B/url copy of keys already
// resident in the index) plus one record and one page per column at a time, not the
// partition.
//
// maxPageRows must be > 0 for the win: it caps the per-column page buffers, so the
// snapshot is multi-page (still a valid, byte-stable .meguri). The host table,
// string arena, and seen-set are small and stay materialized in the shell. The
// resulting file is identical to what Checkpoint would write for the same records
// at the same page cap; only the path to it is bounded.
func (s *Store) CheckpointStreaming(maxPageRows int) error {
	if s.diskIndex {
		// Fold the in-flight discoveries into the repository, then stream the snapshot
		// straight from the sorted repository: no shard merge, no per-url key copy. The
		// repository is already in URLKey order, so the source is a plain forward cursor
		// reading each body from the log at the DRUM-stored offset (doc 04 section 6).
		if err := s.forceMerge(); err != nil {
			return err
		}
		shell := s.snapshotShell()
		return s.commitSnap(len(shell.Strings), func(path string) error {
			src, err := newDrumSource(s)
			if err != nil {
				return err
			}
			defer src.Close()
			return format.StreamEncodeToFile(path, src, maxPageRows, shell, s.dir)
		})
	}
	shell := s.snapshotShell()
	return s.commitSnap(len(shell.Strings), func(path string) error {
		return format.StreamEncodeToFile(path, newMergeSource(s), maxPageRows, shell, s.dir)
	})
}

// snapshotShell builds the non-URL half of the checkpoint partition: the sorted
// host table, the string arena, and the key range. URLs is left nil because the
// streaming encoder sources them from the shard merge, never as a slice.
func (s *Store) snapshotShell() *format.Partition {
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
		Hosts:        hosts,
		Strings:      strs,
	}
}

// shardCursor walks one shard's live keys in ascending URLKey order, materializing
// each record on demand (resident, or read back from the log at its offset) so the
// merge holds at most one record per shard live, never the shard's records as a
// slice. The keys are snapshotted once under the shard lock; reading a record
// re-locks briefly. A checkpoint runs at a quiescent cut, so the key set does not
// move under the cursor; a key that has nonetheless vanished is skipped.
type shardCursor struct {
	s    *Store
	sh   *urlShard
	keys []meguri.URLKey
	i    int
}

func newShardCursor(s *Store, sh *urlShard) *shardCursor {
	sh.mu.RLock()
	keys := make([]meguri.URLKey, 0, len(sh.m))
	for k, loc := range sh.m {
		if loc.tomb {
			continue
		}
		keys = append(keys, k)
	}
	sh.mu.RUnlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
	return &shardCursor{s: s, sh: sh, keys: keys}
}

// record materializes the record for the cursor's current key. ok is false when
// the key has been deleted since the key snapshot or its log frame cannot be read.
func (c *shardCursor) record() (meguri.URLRecord, bool) {
	key := c.keys[c.i]
	c.sh.mu.RLock()
	loc, ok := c.sh.m[key]
	if !ok || loc.tomb {
		c.sh.mu.RUnlock()
		return meguri.URLRecord{}, false
	}
	if loc.rec != nil {
		r := *loc.rec
		c.sh.mu.RUnlock()
		return r, true
	}
	off := loc.off
	c.sh.mu.RUnlock()
	if _, _, _, val, err := c.s.log.readAt(off); err == nil {
		return decodeURL(key, val), true
	}
	return meguri.URLRecord{}, false
}

// cursorHeap is a min-heap of shard cursors ordered by their current head key, the
// k-way merge frontier. Only non-exhausted cursors are ever in the heap.
type cursorHeap []*shardCursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	return h[i].keys[h[i].i].Less(h[j].keys[h[j].i])
}
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)   { *h = append(*h, x.(*shardCursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// mergeSource is a format.URLRecordSource that yields every live URL record in
// global URLKey order by k-way-merging the 256 shard cursors. Each URLKey belongs
// to exactly one shard, so the merged stream has no duplicates and is globally
// sorted, which is what the streaming encoder requires.
type mergeSource struct {
	h *cursorHeap
}

func newMergeSource(s *Store) *mergeSource {
	h := &cursorHeap{}
	for i := range s.shards {
		c := newShardCursor(s, &s.shards[i])
		if len(c.keys) > 0 {
			*h = append(*h, c)
		}
	}
	heap.Init(h)
	return &mergeSource{h: h}
}

func (m *mergeSource) Next() (meguri.URLRecord, bool) {
	for m.h.Len() > 0 {
		c := (*m.h)[0]
		rec, ok := c.record()
		c.i++
		if c.i >= len(c.keys) {
			heap.Pop(m.h)
		} else {
			heap.Fix(m.h, 0)
		}
		if ok {
			return rec, true
		}
		// The key vanished since the snapshot (a delete at the cut edge); skip it
		// and take the next-smallest head.
	}
	return meguri.URLRecord{}, false
}
