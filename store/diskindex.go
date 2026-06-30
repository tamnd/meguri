package store

import (
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/drum"
)

// diskindex.go is the Stage B glue (spec 2072 doc 04): the store-side helpers that
// drive the on-disk DRUM index when DiskIndex is on. The DRUM holds the URL
// location index on disk so the resident shards map is unused for URLs, removing
// the ~80-90 B/url resident index term the size ladder measured. The host table,
// the arena, and the log are unchanged.

// maybeMerge folds the in-flight discovery batch into the repository once enough
// has accumulated (doc 04 section 5.1). The merge is the only place the index
// becomes durable in the repository; between merges the discoveries live in the
// DRUM buckets and the durable log, and a point read finds them through the DRUM's
// overlay. Merge returns the dedup verdicts, which the store's upsert path does not
// need (PutURL is unconditional), so they are dropped.
func (s *Store) maybeMerge() error {
	if s.drum.Unmerged() < s.mergeBatch {
		return nil
	}
	_, err := s.drum.Merge()
	return err
}

// forceMerge folds whatever is in flight, used before a checkpoint or a count so
// the repository reflects every discovery. A no-op when nothing is unmerged.
func (s *Store) forceMerge() error {
	if s.drum.Unmerged() == 0 {
		return nil
	}
	_, err := s.drum.Merge()
	return err
}

// drumSource is a format.URLRecordSource over the merged repository: it walks the
// repository in URLKey order (already the snapshot's row order) and reads each
// record's body from the log at the location the DRUM stored, so a checkpoint
// streams straight from disk without materializing the partition or re-sorting.
// A record whose log frame cannot be read is skipped, the same tolerance the
// shard-merge source has for a key that vanished at the cut edge.
type drumSource struct {
	s *Store
	c *drum.RepoCursor
}

func newDrumSource(s *Store) (*drumSource, error) {
	c, err := s.drum.OpenRepoCursor()
	if err != nil {
		return nil, err
	}
	return &drumSource{s: s, c: c}, nil
}

func (d *drumSource) Next() (meguri.URLRecord, bool) {
	for {
		key, off, _, tomb, ok, err := d.c.Next()
		if err != nil || !ok {
			return meguri.URLRecord{}, false
		}
		if tomb {
			continue
		}
		_, _, _, val, err := d.s.log.readAt(off)
		if err != nil {
			continue
		}
		return decodeURL(key, val), true
	}
}

func (d *drumSource) Close() error { return d.c.Close() }
