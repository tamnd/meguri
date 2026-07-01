package live

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	m "github.com/tamnd/meguri"
)

// StoreManifestName is the fixed filename of the shard map written beside the shard
// .meguri files.
const StoreManifestName = "store.json"

// StoreManifest is the shard map of a sharded live store (Spec 2074 doc 07): the
// ordered list of hostkey-range shards that make up the store, each a self-contained
// .meguri. It is small and read on open, so a driver learns the whole shard set and
// each shard's range and stats without touching a shard body. The ranges tile the
// uint64 hostkey space with no gap and no overlap; shard k owns [HostLo, HostHi) and
// the last shard's HostHi is the max. A store of one shard is the single-file engine's
// N=1 special case.
type StoreManifest struct {
	Version int        `json:"version"`
	Shards  []ShardRef `json:"shards"`
}

// ShardRef is one shard's entry in the store manifest: where its .meguri lives, the
// hostkey range it owns, and the summary stats a build recorded. Generation bumps each
// time the shard is rewritten (compaction, recrawl fold) so the manifest names the
// current file and an interrupted swap is detectable.
type ShardRef struct {
	Index      int    `json:"index"`
	Path       string `json:"path"` // .meguri filename relative to the store dir
	HostLo     uint64 `json:"host_lo"`
	HostHi     uint64 `json:"host_hi"`
	URLCount   int    `json:"url_count"`
	HostCount  int    `json:"host_count"`
	FileBytes  int64  `json:"file_bytes"`
	Generation int    `json:"generation"`
}

// WriteStoreManifest writes m to StoreManifestName under dir.
func WriteStoreManifest(dir string, man StoreManifest) error {
	b, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(dir, StoreManifestName), b, 0o644)
}

// ReadStoreManifest reads the shard map from dir.
func ReadStoreManifest(dir string) (StoreManifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, StoreManifestName))
	if err != nil {
		return StoreManifest{}, err
	}
	var man StoreManifest
	if err := json.Unmarshal(b, &man); err != nil {
		return StoreManifest{}, err
	}
	return man, nil
}

// Route returns the index of the shard that owns hostKey, a binary search over the
// shard ranges. It assumes the shards tile the space in ascending order, which the
// build guarantees.
func (man StoreManifest) Route(hostKey uint64) int {
	i := sort.Search(len(man.Shards), func(i int) bool {
		return man.Shards[i].HostHi > hostKey
	})
	if i >= len(man.Shards) {
		return len(man.Shards) - 1
	}
	return i
}

// Store is an opened sharded live store: the manifest plus one live Engine per shard,
// mapped read-only. It routes a key to its shard's engine for dedup, lookup, and
// schedule, and fans a whole-store read out over all shards. Each shard's mmap is
// reclaimable page cache, so the resident cost is the sum of the per-shard filters,
// not the shard bodies.
type Store struct {
	dir     string
	man     StoreManifest
	engines []*Engine
}

// OpenStore reads the manifest under dir and opens every shard engine. If a shard
// fails to open, the already-opened shards are closed and the error returned, so a
// partial store never leaks maps.
func OpenStore(dir string) (*Store, error) {
	man, err := ReadStoreManifest(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, man: man, engines: make([]*Engine, len(man.Shards))}
	for i, ref := range man.Shards {
		e, err := Open(filepath.Join(dir, ref.Path))
		if err != nil {
			for _, opened := range s.engines[:i] {
				if opened != nil {
					_ = opened.Close()
				}
			}
			return nil, fmt.Errorf("open shard %d (%s): %w", ref.Index, ref.Path, err)
		}
		s.engines[i] = e
	}
	return s, nil
}

// Len is the shard count.
func (s *Store) Len() int { return len(s.engines) }

// Manifest returns the store's shard map.
func (s *Store) Manifest() StoreManifest { return s.man }

// Shard returns the engine for shard i.
func (s *Store) Shard(i int) *Engine { return s.engines[i] }

// Route returns the engine that owns key, the shard a dedup or lookup probe belongs
// to. The pathkey never routes; a host is whole within one shard.
func (s *Store) Route(key m.URLKey) *Engine {
	return s.engines[s.man.Route(key.HostKey)]
}

// URLCount is the total URL count across all shards, summed from the manifest so it
// costs no shard access.
func (s *Store) URLCount() int {
	n := 0
	for _, ref := range s.man.Shards {
		n += ref.URLCount
	}
	return n
}

// BaseProbes sums the slow-path (base-touching) dedup decisions across all shards,
// the store-wide analogue of Engine.BaseProbes.
func (s *Store) BaseProbes() uint64 {
	var n uint64
	for _, e := range s.engines {
		n += e.BaseProbes()
	}
	return n
}

// Close unmaps every shard.
func (s *Store) Close() error {
	var first error
	for _, e := range s.engines {
		if e == nil {
			continue
		}
		if err := e.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
