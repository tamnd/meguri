package seed

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// ShardSet is the write side of a sharded seed: N hostkey-range .seed writers plus
// the manifest that ties them together. It is the one place the shard geometry lives,
// so seedpack (a single-threaded corpus scan) and an external bulk producer (many
// parallel readers, e.g. a Common Crawl parquet fan-out) build byte-compatible seeds
// through the same routing.
//
// The shard count rounds up to a power of two so the top bits of a hostkey select the
// shard, and the ranges tile the whole uint64 hostkey space with no gap or overlap.
// Because the hostkey is a uniform hash of the host, equal-width ranges hold near-equal
// URL counts, and a host maps to exactly one hostkey so its URLs never split across
// shards. The caller supplies the hostkey (meguri.HostKeyOf of the host grouping), which
// keeps this package free of the canon and hashing dependencies and guarantees the key a
// producer routes on is the same key BulkLoad derives at ingest.
//
// Add is safe for concurrent use: each shard has its own writer and its own lock, so P
// producers routing to N shards contend only when two happen to hit the same shard, which
// at N much larger than P is rare. The resident cost is one block buffer per shard plus
// each shard's growing block index, independent of the corpus size.
type ShardSet struct {
	dir       string
	n         int // actual shard count, a power of two >= requested
	bits      int
	shift     uint
	blockSize int
	codec     Codec

	writers []*Writer
	mus     []sync.Mutex
	total   atomic.Uint64
}

// NewShardSet opens n hostkey-range writers under dir (n rounded up to a power of two)
// and returns the set ready for Add. dir must already exist. A failure part-way closes
// the writers opened so far.
func NewShardSet(dir string, shards, blockSize int, codec Codec) (*ShardSet, error) {
	if shards < 1 {
		return nil, fmt.Errorf("seed: shards must be >= 1")
	}
	bits := shardBits(shards)
	n := 1 << bits
	s := &ShardSet{
		dir:       dir,
		n:         n,
		bits:      bits,
		shift:     uint(64 - bits),
		blockSize: blockSize,
		codec:     codec,
		writers:   make([]*Writer, n),
		mus:       make([]sync.Mutex, n),
	}
	for i := range s.writers {
		w, err := NewWriter(filepath.Join(dir, ShardFileName(i)), WriterOptions{
			BlockSize: blockSize, Codec: codec, HostLo: s.HostLo(i), HostHi: s.HostHi(i),
		})
		if err != nil {
			for _, prev := range s.writers[:i] {
				_ = prev.Close()
			}
			return nil, err
		}
		s.writers[i] = w
	}
	return s, nil
}

// shardBits returns the smallest b with 2^b >= shards, so the shard count rounds up to a
// power of two and the top b bits of a hostkey select the shard.
func shardBits(shards int) int {
	b := 0
	for (1 << b) < shards {
		b++
	}
	return b
}

// N is the actual shard count (the requested count rounded up to a power of two).
func (s *ShardSet) N() int { return s.n }

// ShardOf returns the index of the shard that owns hostKey. It matches Manifest.Route: the
// top bits of the hostkey, guarding the single-shard case where the shift would be 64.
func (s *ShardSet) ShardOf(hostKey uint64) int {
	if s.bits == 0 {
		return 0
	}
	return int(hostKey >> s.shift)
}

// HostLo is shard i's inclusive low hostkey bound.
func (s *ShardSet) HostLo(i int) uint64 {
	if s.bits == 0 {
		return 0
	}
	return uint64(i) << s.shift
}

// HostHi is shard i's exclusive high hostkey bound; the last shard's HostHi is the max so
// the ranges tile the whole space.
func (s *ShardSet) HostHi(i int) uint64 {
	if s.bits == 0 || i == s.n-1 {
		return ^uint64(0)
	}
	return uint64(i+1) << s.shift
}

// Add routes url to the shard its hostKey selects and appends it there. hostKey is the
// meguri.HostKeyOf of the URL's host grouping, computed by the caller. It is safe for
// concurrent use across goroutines.
func (s *ShardSet) Add(hostKey uint64, url string) error {
	i := s.ShardOf(hostKey)
	s.mus[i].Lock()
	err := s.writers[i].AddString(url)
	s.mus[i].Unlock()
	if err != nil {
		return err
	}
	s.total.Add(1)
	return nil
}

// Total is the number of URLs added so far across all shards.
func (s *ShardSet) Total() uint64 { return s.total.Load() }

// Close flushes and closes every shard writer, writes the manifest under dir, and returns
// it. After Close the set must not be used again.
func (s *ShardSet) Close() (Manifest, error) {
	metas := make([]ShardMeta, s.n)
	for i, w := range s.writers {
		metas[i] = ShardMeta{
			Index:    i,
			Path:     ShardFileName(i),
			HostLo:   s.HostLo(i),
			HostHi:   s.HostHi(i),
			Records:  w.Records(),
			URLBytes: w.URLByteCount(),
		}
		if err := w.Close(); err != nil {
			return Manifest{}, err
		}
	}
	man := Manifest{
		Version:   1,
		BlockSize: s.blockSize,
		Codec:     s.codec,
		Records:   s.total.Load(),
		Shards:    metas,
	}
	if err := WriteManifest(s.dir, man); err != nil {
		return Manifest{}, err
	}
	return man, nil
}

// ShardFileName is the .seed filename for shard i, the name the store build and the
// manifest both reference.
func ShardFileName(i int) string {
	return fmt.Sprintf("shard-%05d.seed", i)
}
