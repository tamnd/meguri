package live

import (
	"fmt"
	"io"
	"os"
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

// ShardCompactResult pairs a shard's compaction result with its manifest index, whether
// the shard was folded or carried cold, and any error.
type ShardCompactResult struct {
	Index   int
	Result  CompactResult
	Skipped bool // true when the shard had no delta and its file was copied unchanged
	Err     error
}

// CompactSharded folds each shard's delta into its base and writes the store's next
// generation, K shards at a time (Spec 2074 doc 07). deltas is indexed by manifest
// position; a shard whose delta is nil or empty is not re-encoded, its file is copied
// through and its generation kept, which is the design's "a cold range never compacts"
// property. A shard with a delta runs one bounded live.Compact into outDir, so the fold
// is the same per-shard bounded encode the recrawl proved, and pool is the K knob.
//
// The output manifest keeps the input ranges; a folded shard refreshes its counts and
// file bytes and bumps its generation, a carried shard keeps its ref verbatim. It is
// written only if every shard succeeded, so a partial generation is never published.
func CompactSharded(inDir, outDir string, in StoreManifest, deltas []*Delta, mkOpts func(ShardRef) CompactOptions, pool int) (StoreManifest, []ShardCompactResult, error) {
	if len(deltas) != len(in.Shards) {
		return StoreManifest{}, nil, fmt.Errorf("deltas length %d does not match shard count %d", len(deltas), len(in.Shards))
	}
	if pool <= 0 {
		pool = runtime.NumCPU()
	}
	if pool > len(in.Shards) {
		pool = len(in.Shards)
	}
	results := make([]ShardCompactResult, len(in.Shards))

	var wg sync.WaitGroup
	work := make(chan int)
	for range pool {
		wg.Go(func() {
			for i := range work {
				ref := in.Shards[i]
				d := deltas[i]
				if d == nil || d.Len() == 0 {
					err := copyFile(filepath.Join(inDir, ref.Path), filepath.Join(outDir, ref.Path))
					results[i] = ShardCompactResult{Index: ref.Index, Skipped: true, Err: err}
					continue
				}
				opts := mkOpts(ref)
				opts.OutPath = filepath.Join(outDir, ref.Path)
				opts.TmpDir = outDir
				res, err := Compact(filepath.Join(inDir, ref.Path), d, opts)
				results[i] = ShardCompactResult{Index: ref.Index, Result: res, Err: err}
			}
		})
	}
	for i := range in.Shards {
		work <- i
	}
	close(work)
	wg.Wait()

	var man StoreManifest
	man.Version = in.Version
	man.Shards = make([]ShardRef, len(in.Shards))
	var firstErr error
	for i, r := range results {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("shard %d: %w", in.Shards[i].Index, r.Err)
			}
			continue
		}
		ref := in.Shards[i]
		if !r.Skipped {
			ref.URLCount = r.Result.URLCount
			ref.HostCount = r.Result.HostCount
			ref.FileBytes = r.Result.FileBytes
			ref.Generation++
		}
		man.Shards[i] = ref
	}
	if firstErr != nil {
		return StoreManifest{}, results, firstErr
	}
	if err := WriteStoreManifest(outDir, man); err != nil {
		return StoreManifest{}, results, err
	}
	return man, results, nil
}

// copyFile copies src to dst byte for byte, used to carry a cold shard (no delta) into
// the next generation's directory without re-encoding it.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
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

// ShardRecrawlResult pairs a shard's recrawl fold result with its manifest index and
// any error, so the driver reports per-shard outcomes without ordering assumptions.
type ShardRecrawlResult struct {
	Index  int
	Result RecrawlResult
	Err    error
}

// RecrawlSharded folds a crawl outcome into every due row of every shard and writes the
// store's next generation, K shards at a time (Spec 2074 doc 07, the parallel write and
// the direct OOM fix). Each shard's fold is one bounded live.Recrawl into outDir with the
// same filename, so the whole-keyspace encode that overran the box is replaced by K
// bounded per-shard encodes and K (the pool) is the backpressure knob the monolith
// lacked. mkOpts fills the recrawl knobs for a shard; the driver sets OutPath and TmpDir.
// A shard is owned by exactly one worker for its whole fold, the single-writer rule.
//
// The output manifest carries the input ranges unchanged (a fold introduces no keys and
// moves no host across a boundary) with each shard's FileBytes and counts refreshed and
// Generation bumped. It is written only if every shard folded, so a partial generation is
// never published.
func RecrawlSharded(inDir, outDir string, in StoreManifest, mkOpts func(ShardRef) RecrawlOptions, pool int) (StoreManifest, []ShardRecrawlResult, error) {
	if pool <= 0 {
		pool = runtime.NumCPU()
	}
	if pool > len(in.Shards) {
		pool = len(in.Shards)
	}
	results := make([]ShardRecrawlResult, len(in.Shards))

	var wg sync.WaitGroup
	work := make(chan int)
	for range pool {
		wg.Go(func() {
			for i := range work {
				ref := in.Shards[i]
				opts := mkOpts(ref)
				opts.OutPath = filepath.Join(outDir, ref.Path)
				opts.TmpDir = outDir
				res, err := Recrawl(filepath.Join(inDir, ref.Path), opts)
				results[i] = ShardRecrawlResult{Index: ref.Index, Result: res, Err: err}
			}
		})
	}
	for i := range in.Shards {
		work <- i
	}
	close(work)
	wg.Wait()

	var man StoreManifest
	man.Version = in.Version
	man.Shards = make([]ShardRef, len(in.Shards))
	var firstErr error
	for i, r := range results {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("shard %d: %w", in.Shards[i].Index, r.Err)
			}
			continue
		}
		ref := in.Shards[i]
		ref.URLCount = r.Result.URLCount
		ref.HostCount = r.Result.HostCount
		ref.FileBytes = r.Result.FileBytes
		ref.Generation++
		man.Shards[i] = ref
	}
	if firstErr != nil {
		return StoreManifest{}, results, firstErr
	}
	if err := WriteStoreManifest(outDir, man); err != nil {
		return StoreManifest{}, results, err
	}
	return man, results, nil
}
