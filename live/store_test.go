package live

import (
	"fmt"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// makeShardItems builds sorted, in-range items for shard i of an equal-width split of
// the hostkey space into n shards. Keys are placed at the low end of each shard's
// range so they route back to that shard.
func makeShardItems(shard, n, count int) []Item {
	bits := 0
	for (1 << bits) < n {
		bits++
	}
	shift := uint(64 - bits)
	lo := uint64(shard) << shift
	items := make([]Item, count)
	for j := range items {
		hk := lo + uint64(j) // stays inside the shard's range for small counts
		host := fmt.Sprintf("h%016x", hk)
		items[j] = Item{
			Key:  m.URLKey{HostKey: hk, PathKey: uint64(j)},
			URL:  "http://" + host + "/p",
			Host: host,
		}
	}
	return items
}

func hostRange(shard, n int) (lo, hi uint64) {
	bits := 0
	for (1 << bits) < n {
		bits++
	}
	shift := uint(64 - bits)
	lo = uint64(shard) << shift
	if shard == n-1 {
		return lo, ^uint64(0)
	}
	return lo, uint64(shard+1) << shift
}

// TestBuildShardedRoutes builds a multi-shard store and checks that the manifest tiles
// the space, every shard opens, and each key routes to the engine whose range holds it
// and reads back as seen there.
func TestBuildShardedRoutes(t *testing.T) {
	const n = 4
	const perShard = 200
	out := t.TempDir()

	specs := make([]ShardBuildSpec, n)
	allItems := make([][]Item, n)
	for i := range specs {
		lo, hi := hostRange(i, n)
		items := makeShardItems(i, n, perShard)
		allItems[i] = items
		specs[i] = ShardBuildSpec{
			Index:  i,
			HostLo: lo,
			HostHi: hi,
			NewSource: func() (Source, error) {
				return &sliceSource{items: items}, nil
			},
			Opts: BuildOptions{
				TmpDir:       out,
				ExpectedKeys: perShard,
				Codec:        format.CodecZstd,
			},
		}
	}

	man, results, err := BuildSharded(out, specs, 2)
	if err != nil {
		t.Fatalf("BuildSharded: %v", err)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("shard %d build: %v", r.Index, r.Err)
		}
		if r.Result.URLCount != perShard {
			t.Fatalf("shard %d url count = %d, want %d", r.Index, r.Result.URLCount, perShard)
		}
	}

	// Manifest tiles [0, 2^64) ascending.
	if man.Shards[0].HostLo != 0 {
		t.Fatalf("first HostLo = %d, want 0", man.Shards[0].HostLo)
	}
	if last := man.Shards[n-1].HostHi; last != ^uint64(0) {
		t.Fatalf("last HostHi = %d, want max", last)
	}
	for i := 1; i < n; i++ {
		if man.Shards[i].HostLo != man.Shards[i-1].HostHi {
			t.Fatalf("gap between shard %d and %d", i-1, i)
		}
	}

	st, err := OpenStore(out)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer func() { _ = st.Close() }()

	if st.URLCount() != n*perShard {
		t.Fatalf("store url count = %d, want %d", st.URLCount(), n*perShard)
	}

	// Every key routes to the shard whose range holds its hostkey and is seen there.
	for i := range allItems {
		for _, it := range allItems[i] {
			if got := st.man.Route(it.Key.HostKey); got != i {
				t.Fatalf("key hostkey %d routed to shard %d, want %d", it.Key.HostKey, got, i)
			}
			eng := st.Route(it.Key)
			seen, err := eng.Seen(it.Key)
			if err != nil {
				t.Fatalf("Seen: %v", err)
			}
			if !seen {
				t.Fatalf("key %v not seen in its shard %d", it.Key, i)
			}
		}
	}

	// A key from shard 0's range must not be seen in shard 3's engine (routing is not a
	// no-op: shards hold disjoint key sets).
	other := st.Shard(n - 1)
	seen, err := other.Seen(allItems[0][0].Key)
	if err != nil {
		t.Fatalf("cross-shard Seen: %v", err)
	}
	if seen {
		t.Fatalf("shard 0 key reported seen in shard %d", n-1)
	}
}
