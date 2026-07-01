package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	meguri "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/live"
	"github.com/tamnd/meguri/seed"
)

// pathPerturb flips a probe's PathKey so it is guaranteed absent from the base, the
// same constant the single-file dedup stage uses so the two agree on what "a key the
// base does not have" means.
const pathPerturb = 0x8000000000000001

// newShardCmd is the Spec 2074 doc 07 sharded, parallel store driver: it builds and
// exercises a set of hostkey-range .meguri shards in parallel over a bounded worker
// pool, the shape that both fits the box and uses its cores. Each stage is N
// independent per-shard jobs because the seed (doc 08) is pre-sharded by the same key,
// so a shard's seed slice is exactly the keys that route to that shard and no stage
// needs cross-shard coordination.
func newShardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shard",
		Short: "Build and exercise a sharded parallel .meguri store",
		Long:  "shard drives the Spec 2074 doc 07 sharded store: parallel per-shard build and dedup over hostkey-range shards, sized so each shard fits the box and the cores run full.",
	}
	cmd.AddCommand(newShardBuildCmd())
	cmd.AddCommand(newShardDedupCmd())
	return cmd
}

func newShardBuildCmd() *cobra.Command {
	var (
		seedDir string
		store   string
		pool    int
		codec   string
	)
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build one .meguri per seed shard in parallel",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if seedDir == "" || store == "" {
				return fmt.Errorf("--seed and --store are required")
			}
			cd := format.CodecZstd
			if codec == "none" || codec == "raw" {
				cd = format.CodecNone
			}
			return runShardBuild(cmd.OutOrStdout(), seedDir, store, pool, cd)
		},
	}
	cmd.Flags().StringVar(&seedDir, "seed", "", "seed directory holding the .mgs shards and manifest")
	cmd.Flags().StringVar(&store, "store", "", "output directory for the shard .meguri files and store manifest")
	cmd.Flags().IntVar(&pool, "pool", 0, "concurrent shard builds (0 = number of cores)")
	cmd.Flags().StringVar(&codec, "codec", "zstd", "shard body codec: zstd or none")
	return cmd
}

// runShardBuild reads the seed manifest and builds every shard's .meguri with the
// bounded pool, then reports per-shard and aggregate numbers.
func runShardBuild(stdout io.Writer, seedDir, store string, pool int, codec uint8) error {
	man, err := seed.ReadManifest(seedDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(store, 0o755); err != nil {
		return err
	}
	if pool <= 0 {
		pool = runtime.NumCPU()
	}

	specs := make([]live.ShardBuildSpec, len(man.Shards))
	for i, sm := range man.Shards {
		shardPath := filepath.Join(seedDir, sm.Path)
		expect := sm.Records
		specs[i] = live.ShardBuildSpec{
			Index:  sm.Index,
			HostLo: sm.HostLo,
			HostHi: sm.HostHi,
			NewSource: func() (live.Source, error) {
				return newSeedItemSource(shardPath)
			},
			Opts: live.BuildOptions{
				TmpDir:       store,
				ExpectedKeys: expect,
				Codec:        codec,
			},
		}
	}

	start := time.Now()
	sm, results, err := live.BuildSharded(store, specs, pool)
	wall := time.Since(start)
	if err != nil {
		return err
	}

	var urls, hosts int
	var bytes int64
	for _, r := range results {
		urls += r.Result.URLCount
		hosts += r.Result.HostCount
		bytes += r.Result.FileBytes
	}
	fmt.Fprintf(stdout, "shard build: %d shards, pool %d, %d urls, %d hosts, %.2f GiB, %s wall, %s urls/s\n",
		len(sm.Shards), pool, urls, hosts, float64(bytes)/(1<<30), wall.Round(time.Millisecond), humanRate(urls, wall))
	for _, r := range results {
		ref := sm.Shards[r.Index]
		fmt.Fprintf(stdout, "  shard %05d  %d urls  %d hosts  %.1f MiB\n",
			r.Index, ref.URLCount, ref.HostCount, float64(ref.FileBytes)/(1<<20))
	}
	return nil
}

func newShardDedupCmd() *cobra.Command {
	var (
		seedDir string
		store   string
		workers int
	)
	cmd := &cobra.Command{
		Use:   "dedup",
		Short: "Replay every shard's seed as fresh discoveries against its engine, in parallel",
		Long:  "dedup opens each shard's engine and replays that shard's seed slice as perturbed (guaranteed-absent) probes, the intake case, with at most --workers shards resident at once so the resident filter cost is bounded by the pool, not the shard count.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if seedDir == "" || store == "" {
				return fmt.Errorf("--seed and --store are required")
			}
			return runShardDedup(cmd.OutOrStdout(), seedDir, store, workers)
		},
	}
	cmd.Flags().StringVar(&seedDir, "seed", "", "seed directory holding the .mgs shards")
	cmd.Flags().StringVar(&store, "store", "", "store directory holding the shard .meguri files and manifest")
	cmd.Flags().IntVar(&workers, "workers", 0, "concurrent shard dedup workers (0 = number of cores)")
	return cmd
}

// runShardDedup fans the per-shard dedup replay across a bounded pool. Each worker
// opens one shard's seed and engine, probes every key perturbed so the resident filter
// answers without a file page, then closes both, so at most workers filters are
// resident at once.
func runShardDedup(stdout io.Writer, seedDir, store string, workers int) error {
	man, err := live.ReadStoreManifest(store)
	if err != nil {
		return err
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if workers > len(man.Shards) {
		workers = len(man.Shards)
	}

	var probes, baseProbes, hits atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	start := time.Now()
	var wg sync.WaitGroup
	work := make(chan int)
	for range workers {
		wg.Go(func() {
			for i := range work {
				p, b, h, e := dedupOneShard(seedDir, store, man.Shards[i])
				probes.Add(p)
				baseProbes.Add(b)
				hits.Add(h)
				if e != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("shard %d: %w", man.Shards[i].Index, e)
					}
					errMu.Unlock()
				}
			}
		})
	}
	for i := range man.Shards {
		work <- i
	}
	close(work)
	wg.Wait()
	wall := time.Since(start)
	if firstErr != nil {
		return firstErr
	}

	p := probes.Load()
	b := baseProbes.Load()
	fmt.Fprintf(stdout, "shard dedup: %d probes, %d workers, %s wall, %s probes/s\n",
		p, workers, wall.Round(time.Millisecond), humanRate(int(p), wall))
	fmt.Fprintf(stdout, "  %d filter-miss (resident, no file), %d base-confirm (%.3f%% FP), %d present\n",
		p-b, b, 100*float64(b)/float64(max64(p, 1)), hits.Load())
	return nil
}

// dedupOneShard opens one shard's seed and engine and replays the seed as perturbed
// probes, returning the probe, base-confirm, and present counts. It is the unit of
// parallel dedup work; nothing it touches is shared.
func dedupOneShard(seedDir, store string, ref live.ShardRef) (probes, base, hits uint64, err error) {
	src, err := newSeedItemSource(filepath.Join(seedDir, seedShardName(ref.Index)))
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = src.Close() }()
	eng, err := live.Open(filepath.Join(store, ref.Path))
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = eng.Close() }()

	for {
		it, ok, e := src.Next()
		if e != nil {
			return probes, base, hits, e
		}
		if !ok {
			break
		}
		probes++
		probe := it.Key
		probe.PathKey ^= pathPerturb
		hit, se := eng.Seen(probe)
		if se != nil {
			return probes, base, hits, se
		}
		if hit {
			hits++
		}
	}
	return probes, eng.BaseProbes(), hits, nil
}

// seedItemSource adapts a .mgs shard reader to a live.Source, deriving each URL's key
// the way the engine keys it. It is the one place a seed URL becomes a live.Item; the
// build sorts by key, so the seed's host-string order does not need to match key order.
type seedItemSource struct {
	r  *seed.Reader
	bi int
	br *seed.BlockReader
}

func newSeedItemSource(path string) (*seedItemSource, error) {
	r, err := seed.Open(path)
	if err != nil {
		return nil, err
	}
	return &seedItemSource{r: r}, nil
}

func (s *seedItemSource) Next() (live.Item, bool, error) {
	for {
		if s.br == nil {
			if s.bi >= s.r.Blocks() {
				return live.Item{}, false, nil
			}
			br, err := s.r.BlockReader(s.bi)
			if err != nil {
				return live.Item{}, false, err
			}
			s.br = br
			s.bi++
		}
		u, ok := s.br.Next()
		if !ok {
			s.br = nil
			continue
		}
		url := string(u)
		host := frontier.HostOf(url)
		if host == "" {
			continue
		}
		key := meguri.URLKey{
			HostKey: meguri.HostKeyOf(host),
			PathKey: meguri.PathKeyOf(frontier.PathOf(url)),
		}
		return live.Item{Key: key, URL: url, Host: host}, true, nil
	}
}

func (s *seedItemSource) Close() error { return s.r.Close() }

func seedShardName(i int) string { return fmt.Sprintf("shard-%05d.mgs", i) }

func humanRate(n int, d time.Duration) string {
	if d <= 0 {
		return "inf"
	}
	return humanCount(float64(n) / d.Seconds())
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
