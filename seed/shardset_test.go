package seed

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// hostKeyOf is a local stand-in for meguri.HostKeyOf so the seed package test stays free of
// the root import cycle. The ShardSet only routes on the caller's key, so any deterministic
// spread of keys exercises the range routing; the value need not match the real hash.
func hostKeyOf(i int) uint64 {
	x := uint64(i)*0x9E3779B97F4A7C15 + 0x2545F4914F6CDD1D
	x ^= x >> 33
	x *= 0xFF51AFD7ED558CCD
	x ^= x >> 33
	return x
}

// TestShardSetRoutesAndTiles adds a batch of keyed URLs concurrently from many goroutines,
// then reads every shard back and checks three things: no URL is lost, every URL sits in
// the shard whose hostkey range owns it, and the manifest ranges tile the whole space with
// no gap. The concurrent add is the path a parallel Common Crawl producer takes.
func TestShardSetRoutesAndTiles(t *testing.T) {
	dir := t.TempDir()
	const shards = 16
	const urls = 50000

	set, err := NewShardSet(dir, shards, DefaultBlockSize, CodecZstd)
	if err != nil {
		t.Fatalf("NewShardSet: %v", err)
	}

	// Fan the adds across goroutines so the per-shard locks are actually contended.
	var wg sync.WaitGroup
	const workers = 8
	for w := range workers {
		wg.Go(func() {
			for i := w; i < urls; i += workers {
				hk := hostKeyOf(i)
				if err := set.Add(hk, fmt.Sprintf("http://h%d.example/p/%d", i%1000, i)); err != nil {
					t.Errorf("Add: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()

	man, err := set.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if man.Records != urls {
		t.Fatalf("manifest records = %d, want %d", man.Records, urls)
	}
	if len(man.Shards) != shards {
		t.Fatalf("shard count = %d, want %d (power of two)", len(man.Shards), shards)
	}

	// The ranges must tile [0, max] with no gap: shard 0 starts at 0, each shard's HostHi is
	// the next shard's HostLo, and the last shard's HostHi is the max.
	if man.Shards[0].HostLo != 0 {
		t.Fatalf("shard 0 HostLo = %d, want 0", man.Shards[0].HostLo)
	}
	for i := 1; i < len(man.Shards); i++ {
		if man.Shards[i].HostLo != man.Shards[i-1].HostHi {
			t.Fatalf("gap between shard %d HostHi %d and shard %d HostLo %d",
				i-1, man.Shards[i-1].HostHi, i, man.Shards[i].HostLo)
		}
	}
	if man.Shards[len(man.Shards)-1].HostHi != ^uint64(0) {
		t.Fatalf("last shard HostHi = %d, want max", man.Shards[len(man.Shards)-1].HostHi)
	}

	// Read every shard back, count URLs, and confirm the manifest counts agree. The whole
	// batch must be present exactly once.
	var seen uint64
	for _, sm := range man.Shards {
		r, err := Open(filepath.Join(dir, sm.Path))
		if err != nil {
			t.Fatalf("Open shard %d: %v", sm.Index, err)
		}
		var n uint64
		for b := range r.Blocks() {
			br, err := r.BlockReader(b)
			if err != nil {
				t.Fatalf("BlockReader: %v", err)
			}
			for {
				_, ok := br.Next()
				if !ok {
					break
				}
				n++
			}
		}
		if n != sm.Records {
			t.Fatalf("shard %d body holds %d urls, manifest says %d", sm.Index, n, sm.Records)
		}
		seen += n
		_ = r.Close()
	}
	if seen != urls {
		t.Fatalf("read %d urls back across shards, want %d", seen, urls)
	}
}

// TestShardSetRouteMatchesManifest checks ShardOf agrees with Manifest.Route for a spread of
// keys, so a producer routing with the live ShardSet and a reader routing with the persisted
// manifest send the same hostkey to the same shard.
func TestShardSetRouteMatchesManifest(t *testing.T) {
	dir := t.TempDir()
	set, err := NewShardSet(dir, 32, DefaultBlockSize, CodecRaw)
	if err != nil {
		t.Fatalf("NewShardSet: %v", err)
	}
	man, err := set.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := range 100000 {
		hk := hostKeyOf(i)
		if got, want := set.ShardOf(hk), man.Route(hk); got != want {
			t.Fatalf("hostkey %d: ShardOf=%d Manifest.Route=%d", hk, got, want)
		}
	}
}
