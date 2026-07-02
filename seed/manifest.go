package seed

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// ManifestName is the fixed filename of the shard map written beside the shards.
const ManifestName = "manifest.json"

// Manifest is the shard map: the ordered list of hostkey-range shards that make up
// one sharded seed (and, later, the store built from it). It is small and read on
// open, so a driver learns the whole shard set and each shard's range without
// touching a shard body. The ranges tile the uint64 hostkey space with no gap and
// no overlap: shard k covers [HostLo, HostHi), the last shard's HostHi is the max.
type Manifest struct {
	Version   int         `json:"version"`
	BlockSize int         `json:"block_size"`
	Codec     Codec       `json:"codec"`
	Records   uint64      `json:"records"`
	Shards    []ShardMeta `json:"shards"`
}

// ShardMeta is one shard's entry in the manifest.
type ShardMeta struct {
	Index    int    `json:"index"`
	Path     string `json:"path"` // filename relative to the manifest dir
	HostLo   uint64 `json:"host_lo"`
	HostHi   uint64 `json:"host_hi"`
	Records  uint64 `json:"records"`
	URLBytes uint64 `json:"url_bytes"`
}

// WriteManifest writes m to ManifestName under dir.
func WriteManifest(dir string, m Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(fmt.Sprintf("%s/%s", dir, ManifestName), b, 0o644)
}

// ReadManifest reads the shard map from dir.
func ReadManifest(dir string) (Manifest, error) {
	b, err := os.ReadFile(fmt.Sprintf("%s/%s", dir, ManifestName))
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// Route returns the index of the shard that owns hostKey, a binary search over the
// shard ranges. It assumes the shards tile the space in ascending order, which
// WriteManifest's producer guarantees.
func (m Manifest) Route(hostKey uint64) int {
	i := sort.Search(len(m.Shards), func(i int) bool {
		return m.Shards[i].HostHi > hostKey
	})
	if i >= len(m.Shards) {
		return len(m.Shards) - 1
	}
	return i
}
