package live

import (
	"slices"

	m "github.com/tamnd/meguri"
)

// DeltaEntry is one pending write against the live store: a discovery (a key not in
// the base) or a recrawl update (a key already in the base, re-fetched with fresh
// crawl state). Rec is the full record to store, so the caller owns the field merge
// (a recrawler reads the base row, updates NextDue and the crawl counters, and hands
// the whole post-update record here); Compact re-interns URL and Host into the new
// file's arena and overwrites Rec.URLRef, so the caller leaves those unset. The
// string fields travel with the entry because the base arena is not addressable by
// the new file.
type DeltaEntry struct {
	Rec  m.URLRecord // full post-write record; URLRef is assigned by Compact
	URL  string      // canonical URL string, re-interned into the new arena
	Host string      // host grouping string, interned if the host is new
}

// Delta is the bounded resident write buffer of spec 2073 doc 08 Stage 2: discoveries
// and recrawl updates accumulate here, and a compaction folds them into the base file
// to produce the next file generation. It is sorted by URLKey only at flush, so Put is
// O(1) and the sort is paid once per compaction. The residency budget is the caller's:
// Bytes reports the approximate resident cost so the caller compacts before the delta
// outgrows the box, the same way the base filter is sized ahead.
type Delta struct {
	entries []DeltaEntry
	bytes   int
	hosts   map[uint64]string
}

// NewDelta returns an empty write buffer.
func NewDelta() *Delta {
	return &Delta{hosts: make(map[uint64]string)}
}

// Put appends a pending write. A later Put for the same key supersedes an earlier one;
// the flush keeps the last write per key, so an update after an insert in the same
// batch folds to a single row.
func (d *Delta) Put(e DeltaEntry) {
	d.entries = append(d.entries, e)
	// Approximate resident cost: the entry struct plus its two strings. The URLKey and
	// the record are fixed width; the strings dominate at scale.
	d.bytes += len(e.URL) + len(e.Host) + int(rowWidth) + 48
	if _, ok := d.hosts[e.Rec.URLKey.HostKey]; !ok {
		d.hosts[e.Rec.URLKey.HostKey] = e.Host
	}
}

// Len is the number of buffered writes, before same-key deduplication.
func (d *Delta) Len() int { return len(d.entries) }

// Bytes is the approximate resident cost of the buffer, the flush trigger.
func (d *Delta) Bytes() int { return d.bytes }

// sorted returns the entries in ascending URLKey order, keeping the last write per
// key so a batch that discovers then updates a URL folds to one row. The sort is
// stable on key so the last occurrence wins after the dedup sweep.
func (d *Delta) sorted() []DeltaEntry {
	out := slices.Clone(d.entries)
	// Sort by key ascending; on a tie keep the original order so the later Put is last.
	slices.SortStableFunc(out, func(a, b DeltaEntry) int {
		return a.Rec.URLKey.Compare(b.Rec.URLKey)
	})
	// Sweep out duplicate keys, keeping the last (the fresher write).
	w := 0
	for i := range out {
		if i+1 < len(out) && out[i].Rec.URLKey == out[i+1].Rec.URLKey {
			continue
		}
		out[w] = out[i]
		w++
	}
	return out[:w]
}
