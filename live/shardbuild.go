package live

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
)

// ShardBuildSpec describes one shard to build: its index and hostkey range, a factory
// that opens a fresh Source over that shard's seed slice, and the BulkLoad options.
// The driver sets Opts.Path, so a caller fills only the build knobs (codec, filter
// budget, expected keys, stamps). NewSource is a factory, not a Source, so the driver
// opens the seed only when a worker picks the shard up, keeping at most pool sources
// live at once.
type ShardBuildSpec struct {
	Index     int
	HostLo    uint64
	HostHi    uint64
	NewSource func() (Source, error)
	Opts      BuildOptions
}

// ShardBuildResult pairs a shard's build result with its index and any error, so the
// driver can report per-shard outcomes without ordering assumptions.
type ShardBuildResult struct {
	Index  int
	Result BuildResult
	Err    error
}

// BuildSharded builds every shard's .meguri from its seed slice with a bounded worker
// pool, then writes the store manifest under outDir (Spec 2074 doc 07, the parallel
// build). At most pool shards build at once, so the box runs full but not
// oversubscribed and the resident scratch is bounded by pool per-shard builds, not by
// the shard count. Each shard is one worker for its whole build, which is the
// single-writer-per-shard rule: no two workers ever touch the same shard file.
//
// A shard's output is outDir/shard-NNNNN.meguri. The manifest is written only if every
// shard built; a failed shard is returned in the per-shard results and aborts the
// manifest so a partial store is never published.
func BuildSharded(outDir string, specs []ShardBuildSpec, pool int) (StoreManifest, []ShardBuildResult, error) {
	if pool <= 0 {
		pool = runtime.NumCPU()
	}
	if pool > len(specs) {
		pool = len(specs)
	}
	results := make([]ShardBuildResult, len(specs))

	var wg sync.WaitGroup
	work := make(chan int)
	for range pool {
		wg.Go(func() {
			for i := range work {
				results[i] = buildOneShard(outDir, specs[i])
			}
		})
	}
	for i := range specs {
		work <- i
	}
	close(work)
	wg.Wait()

	var man StoreManifest
	man.Version = 1
	man.Shards = make([]ShardRef, len(specs))
	var firstErr error
	for i, r := range results {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("shard %d: %w", specs[i].Index, r.Err)
			}
			continue
		}
		sp := specs[i]
		man.Shards[i] = ShardRef{
			Index:     sp.Index,
			Path:      shardFileName(sp.Index),
			HostLo:    sp.HostLo,
			HostHi:    sp.HostHi,
			URLCount:  r.Result.URLCount,
			HostCount: r.Result.HostCount,
			FileBytes: r.Result.FileBytes,
		}
	}
	if firstErr != nil {
		return StoreManifest{}, results, firstErr
	}
	if err := WriteStoreManifest(outDir, man); err != nil {
		return StoreManifest{}, results, err
	}
	return man, results, nil
}

// buildOneShard opens the shard's source and runs a single BulkLoad into its shard
// file. It is the unit of parallel work; nothing it touches is shared with another
// shard's build.
func buildOneShard(outDir string, sp ShardBuildSpec) ShardBuildResult {
	src, err := sp.NewSource()
	if err != nil {
		return ShardBuildResult{Index: sp.Index, Err: err}
	}
	opts := sp.Opts
	opts.Path = filepath.Join(outDir, shardFileName(sp.Index))
	res, err := BulkLoad(src, opts)
	if closer, ok := src.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	return ShardBuildResult{Index: sp.Index, Result: res, Err: err}
}

// shardFileName is the .meguri filename for shard i, matching the seed's shard-NNNNN
// naming so a store dir reads alongside its seed dir.
func shardFileName(i int) string {
	return fmt.Sprintf("shard-%05d.meguri", i)
}
