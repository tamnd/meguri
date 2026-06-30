package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/scale"
	"github.com/tamnd/meguri/store"
)

// scaleDrainFetcher is the offline fetcher the scale runner binds so the run stage
// measures the frontier, not the network: every dispatched URL is marked crawled
// with a 200 at the current epoch-hour, no body, no links. It is the same idea as
// run.go's drainFetcher, kept local so the scale path drives the real engine loop.
type scaleDrainFetcher struct{}

func (scaleDrainFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	return meguri.Outcome{
		URLKey:     req.URLKey,
		HTTPStatus: 200,
		FetchedAt:  uint32(time.Now().Unix() / 3600),
	}, nil
}

// newScaleCmd is the timed, resource-instrumented runner (Spec scale doc 03). It
// reads a pinned corpus, drives the real seed and run paths under the scale
// harness, captures wall and CPU time, peak RSS and heap, allocations per URL,
// disk bytes, and a CPU and heap profile per run, and writes one JSON Result plus
// a human summary to the output directory. The deterministic size facts stay the
// job of `meguri bench`; this measures the numbers a clock and a box produce.
func newScaleCmd() *cobra.Command {
	var (
		input            string
		profile          string
		box              string
		commit           string
		outDir           string
		doSeed           bool
		doRun            bool
		doInspect        bool
		doIngest         bool
		residentBudget   int
		seedMode         string
		streamCheckpoint bool
		pageRows         int
		spillArena       bool
		arenaBudget      int64
	)
	cmd := &cobra.Command{
		Use:   "scale",
		Short: "Run the timed, resource-instrumented scale harness over a corpus",
		Long:  "scale drives the real seed and run paths over a pinned ccrawl corpus under the scale harness, capturing wall and CPU time, peak RSS and heap, allocations per URL, disk bytes, and a CPU and heap profile per run. It writes a JSON result and a human summary to --out. Stamp --box (the box of record) and a real --input corpus for a number of record; without them the run is a smoke run, not a number of record.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if input == "" {
				return fmt.Errorf("--input is required (a pinned ccrawl CDX JSONL corpus)")
			}
			if outDir == "" {
				outDir = "scale-results"
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				return err
			}

			// Only the seed, run, and inspect stages need the whole corpus resident
			// (they iterate it more than once and measure the engine, not the parser).
			// The ingest stage streams the corpus a line at a time, so a pure-ingest
			// run never materializes the ~12 GB slice that, on top of a resident seed
			// frontier, is what pins a 100M run past a 64 GB box.
			needLines := doSeed || doRun || doInspect
			var lines []corpusLine
			if needLines {
				loaded, lerr := readCorpus(input)
				if lerr != nil {
					return lerr
				}
				if len(loaded) == 0 {
					return fmt.Errorf("corpus %s is empty", input)
				}
				lines = loaded
			}

			pprofDir := filepath.Join(outDir, "pprof")
			if err := os.MkdirAll(pprofDir, 0o755); err != nil {
				return err
			}

			result := scale.Result{
				Profile: profile,
				Provenance: scale.Provenance{
					Box:    box,
					Commit: commit,
					Corpus: input,
					GOOS:   runtime.GOOS,
					GOARCH: runtime.GOARCH,
					NumCPU: runtime.NumCPU(),
				},
				StartedAt: time.Now().UTC().Format(time.RFC3339),
				PprofDir:  pprofDir,
			}

			tag := profile
			if tag == "" {
				tag = "run"
			}

			// seedInto fills a fresh frontier from the corpus, either the per-key
			// Seed loop (the O(n^2) intake the profiler flagged, kept as the "before"
			// baseline) or the batched SeedBatch path (the fix, one DRUM merge per
			// bucket). Both build an identical frontier; only the intake cost differs,
			// which is exactly the number the paired before/after measures.
			seedInto := func(fr *frontier.Frontier) {
				if seedMode == "loop" {
					for _, ln := range lines {
						fr.Seed(ln.url, ln.host, 0.5, 0, 0, 10)
					}
					return
				}
				const window = 1 << 16
				buf := make([]frontier.SeedSpec, 0, window)
				for _, ln := range lines {
					buf = append(buf, frontier.SeedSpec{URL: ln.url, Host: ln.host, Priority: 0.5, CrawlDelay: 10})
					if len(buf) == window {
						fr.SeedBatch(buf)
						buf = buf[:0]
					}
				}
				fr.SeedBatch(buf)
			}

			// Seed stage: frontier.New, intake every URL, CheckpointBytes. The CPU
			// profile wraps it so the intake hot path (canonicalize, hash, dedup,
			// append, then encode) is captured for doc 05's cross-size comparison.
			if doSeed {
				var seeded *frontier.Frontier
				var heldHeap uint64
				seedStage, err := profiledStage(pprofDir, "seed", tag, func() (scale.StageResult, error) {
					return scale.StageResultFromSeed(len(lines), func() (uint64, error) {
						fr := frontier.New(1, 0)
						seedInto(fr)
						// Held residency: the live heap the built frontier holds, measured
						// before the checkpoint encode so it is the resident footprint the
						// budget caps, not the one-shot encode spike the peak RSS captures.
						heldHeap = scale.HeldHeap(fr)
						blob, e := fr.CheckpointBytes()
						if e != nil {
							return 0, e
						}
						seeded = fr
						ckptPath := filepath.Join(outDir, fmt.Sprintf("%s.seed.meguri", tag))
						if e := os.WriteFile(ckptPath, blob, 0o644); e != nil {
							return 0, e
						}
						return uint64(len(blob)), nil
					})
				})
				if err != nil {
					return fmt.Errorf("seed stage: %w", err)
				}
				seedStage.Mem.HeldHeapInUse = heldHeap
				seedStage.Notes = fmt.Sprintf("%d urls resident after dedup, held heap %.1f bytes/url",
					seeded.Len(), float64(heldHeap)/float64(seeded.Len()))
				result.Stages = append(result.Stages, seedStage)
			}

			// Inspect stage: read the checkpoint the seed stage wrote back off disk
			// and decode every column. This is the only stage that touches the disk
			// read path and the full columnar decode (zstd, FSST, the urlkey and host
			// columns), so it fills the bytes_read side of the ledger and the decode
			// throughput the recovery and serve stages build on. It reads the whole
			// file, the cold-restore cost, not the tail-only inspect cmd shortcut.
			if doInspect {
				ckptPath := filepath.Join(outDir, fmt.Sprintf("%s.seed.meguri", tag))
				inspectStage, err := profiledStage(pprofDir, "inspect", tag, func() (scale.StageResult, error) {
					var decoded int
					res, e := scale.StageResultFromInspect(0, func() (uint64, error) {
						raw, e := os.ReadFile(ckptPath)
						if e != nil {
							return 0, e
						}
						part, e := format.Decode(raw)
						if e != nil {
							return 0, e
						}
						decoded = len(part.URLs)
						return uint64(len(raw)), nil
					})
					if e != nil {
						return res, e
					}
					// Restamp the URL denominator now that the decode reported it, so
					// the per-URL decode throughput uses the real reconstructed count.
					res = scale.WithURLs(res, decoded)
					return res, nil
				})
				if err != nil {
					return fmt.Errorf("inspect stage: %w", err)
				}
				inspectStage.Notes = fmt.Sprintf("%d urls decoded from checkpoint", inspectStage.URLs)
				result.Stages = append(result.Stages, inspectStage)
			}

			// Run stage: drive the staged engine loop with the offline drain
			// fetcher under a logical clock, so politeness waits collapse and the
			// scheduler selection path is what we measure. Re-seed a fresh frontier
			// so the run stage starts from the same input the seed stage built.
			if doRun {
				runStage, err := profiledStage(pprofDir, "run", tag, func() (scale.StageResult, error) {
					fr := frontier.New(1, 0)
					seedInto(fr)
					resident := fr.Len()
					return scale.StageResultFromRun(resident, func() (uint64, error) {
						eng := engine.New(fr, engine.Config{
							Fetcher:    scaleDrainFetcher{},
							Clock:      engine.NewLogicalClock(uint32(time.Now().Unix())),
							UntilEmpty: true,
						})
						if e := eng.Run(cmd.Context()); e != nil {
							return 0, e
						}
						return 0, nil
					})
				})
				if err != nil {
					return fmt.Errorf("run stage: %w", err)
				}
				result.Stages = append(result.Stages, runStage)
			}

			// Ingest stage: drive the durable store path with a resident budget so
			// the resident heap is bounded while the corpus grows past it. This is the
			// only path that bounds memory for a 100M single-box run: the seed and run
			// stages above hold the whole frontier resident (the 10M ceiling), while
			// this stage caps the resident records at --resident-budget and spills the
			// cold bulk to the log, the larger-than-memory residency of doc 11 and the
			// 100M efficiency ceiling of scale doc 12. It builds the same records the
			// frontier seed path builds (StatusScheduled, SourceSeed, the host record
			// per distinct host) but writes them straight to the store so a cold record
			// never has to be resident. It measures held heap (which should flatten at
			// the budget), resident count, on-disk bytes, and a durable checkpoint.
			if doIngest {
				storeDir := filepath.Join(outDir, fmt.Sprintf("%s.store", tag))
				if err := os.RemoveAll(storeDir); err != nil {
					return err
				}
				var (
					ingestHeld     uint64
					ingestResident int
					ingestURLs     int
					ingestDisk     uint64
					ingestSnap     uint64
				)
				ingestStage, err := profiledStage(pprofDir, "ingest", tag, func() (scale.StageResult, error) {
					return scale.StageResultFromIngest(len(lines), func() (uint64, error) {
						st, e := store.Open(storeDir, store.Options{
							Durability:     store.DurabilityNormal,
							ResidentBudget: residentBudget,
							SpillArena:     spillArena,
							ArenaBudget:    arenaBudget,
						})
						if e != nil {
							return 0, e
						}
						// Host dedup is the one resident structure ingest keeps: a set of
						// distinct host keys (millions, not the 100M URL count), so it stays
						// small. Everything per-URL spills to the store's log.
						seen := make(map[uint64]struct{}, 1<<16)
						ingestURLs = 0
						ingestOne := func(ln corpusLine) error {
							ingestURLs++
							hk := meguri.HostKeyOf(ln.host)
							if _, ok := seen[hk]; !ok {
								seen[hk] = struct{}{}
								hostRef, he := st.Intern(ln.host)
								if he != nil {
									return he
								}
								if _, he := st.PutHost(&meguri.HostRecord{
									HostKey:    hk,
									HostRef:    hostRef,
									Grouping:   meguri.GroupFullHost,
									CrawlDelay: 100,
								}); he != nil {
									return he
								}
							}
							urlRef, ue := st.Intern(ln.url)
							if ue != nil {
								return ue
							}
							key := meguri.URLKey{HostKey: hk, PathKey: meguri.PathKeyOf(frontier.PathOf(ln.url))}
							if _, pe := st.PutURL(&meguri.URLRecord{
								URLKey:          key,
								HostKey:         hk,
								Status:          meguri.StatusScheduled,
								Priority:        0.5,
								URLRef:          urlRef,
								DiscoverySource: meguri.SourceSeed,
							}); pe != nil {
								return pe
							}
							return nil
						}
						// When a resident stage already loaded the corpus, reuse the slice;
						// otherwise stream it off disk so the ingest holds no corpus in RAM.
						if lines != nil {
							for _, ln := range lines {
								if e := ingestOne(ln); e != nil {
									return 0, e
								}
							}
						} else if e := streamCorpus(input, ingestOne); e != nil {
							return 0, e
						}
						// Held residency: the live heap the store holds with the budget
						// in force, measured before the checkpoint so it is the capped
						// resident footprint, not the checkpoint encode spike.
						ingestHeld = scale.HeldHeap(st)
						ingestResident = st.Resident()
						// A zero budget never tracks the resident counter (every record
						// stays resident), so the resident count is the full live set.
						if residentBudget <= 0 {
							ingestResident = st.URLCount()
						}
						ingestDisk = dirSize(storeDir)
						// The bounded checkpoint (spec 2072 D9): stream the snapshot
						// through the 256-shard k-way merge so the encode never
						// materializes the partition, the transient that OOMs a 64 GB
						// box at 100M. Byte-identical to the materializing path at the
						// same page cap (TestCheckpointStreamingMatchesMaterialized).
						if streamCheckpoint {
							if ce := st.CheckpointStreaming(pageRows); ce != nil {
								return 0, ce
							}
						} else if ce := st.Checkpoint(); ce != nil {
							return 0, ce
						}
						ingestSnap = dirSize(storeDir)
						if ce := st.Close(); ce != nil {
							return 0, ce
						}
						return ingestDisk, nil
					})
				})
				if err != nil {
					return fmt.Errorf("ingest stage: %w", err)
				}
				// A streamed ingest measured with urls=0 (the count is known only after
				// the pass); restamp the real URL count so the per-URL ratios are right.
				if lines == nil {
					ingestStage = scale.WithURLs(ingestStage, ingestURLs)
				}
				ingestStage.Mem.HeldHeapInUse = ingestHeld
				budgetNote := "unbounded"
				if residentBudget > 0 {
					budgetNote = fmt.Sprintf("budget %d", residentBudget)
				}
				ingestStage.Notes = fmt.Sprintf(
					"%s: %d resident of %d urls, held heap %.1f B/url, log %.1f B/url, checkpoint total %.1f B/url",
					budgetNote, ingestResident, ingestStage.URLs,
					float64(ingestHeld)/float64(max(ingestResident, 1)),
					float64(ingestDisk)/float64(max(ingestStage.URLs, 1)),
					float64(ingestSnap)/float64(max(ingestStage.URLs, 1)))
				result.Stages = append(result.Stages, ingestStage)
			}

			// Write the JSON ledger entry and the human summary.
			jsonPath := filepath.Join(outDir, fmt.Sprintf("result.%s.%s.json", tag, shortCommit(commit)))
			jf, err := os.Create(jsonPath)
			if err != nil {
				return err
			}
			if err := result.WriteJSON(jf); err != nil {
				_ = jf.Close()
				return err
			}
			_ = jf.Close()

			result.WriteHuman(cmd.OutOrStdout())
			fmt.Fprintf(cmd.OutOrStdout(), "\nwrote %s\n", jsonPath)
			return nil
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "pinned ccrawl CDX JSONL corpus to run (required)")
	cmd.Flags().StringVar(&profile, "profile", "", "profile label (10k, 100k, 1m, 10m)")
	cmd.Flags().StringVar(&box, "box", "", "box-of-record label (e.g. server2); empty means a smoke run, not a number of record")
	cmd.Flags().StringVar(&commit, "commit", "", "meguri commit the run was built from")
	cmd.Flags().StringVar(&outDir, "out", "", "directory for results and profiles (default scale-results)")
	cmd.Flags().BoolVar(&doSeed, "seed", true, "drive the seed stage (resident frontier intake); set false for a pure bounded ingest run")
	cmd.Flags().BoolVar(&doRun, "run", true, "drive the run stage (engine drain) in addition to seed")
	cmd.Flags().BoolVar(&doInspect, "inspect", true, "drive the inspect stage (read the checkpoint back and decode it) after seed")
	cmd.Flags().BoolVar(&doIngest, "ingest", false, "drive the ingest stage (durable store path with a resident budget, the bounded-memory 100M path)")
	cmd.Flags().IntVar(&residentBudget, "resident-budget", 0, "max resident URL records during ingest, 0 = unbounded (the budget the held heap flattens at)")
	cmd.Flags().StringVar(&seedMode, "seed-mode", "batch", "seed intake path: batch (DRUM merge, the default) or loop (per-key, the pre-fix baseline)")
	cmd.Flags().BoolVar(&streamCheckpoint, "stream-checkpoint", false, "ingest checkpoint via the bounded k-way shard-merge stream (spec 2072 D9) instead of materializing the partition")
	cmd.Flags().IntVar(&pageRows, "page-rows", 65536, "column page-row cap for the streaming checkpoint (must be > 0 for the bounded transient)")
	cmd.Flags().BoolVar(&spillArena, "spill-arena", false, "spill the canonical-URL string arena to disk read through a bounded LRU (spec 2072 Stage A), removing ~70 B/url from the held heap")
	cmd.Flags().Int64Var(&arenaBudget, "arena-budget", 0, "resident byte ceiling for the spilled arena LRU (B_arena); 0 picks the 64 MiB default")
	return cmd
}

// corpusLine is the one capture the scale runner seeds from.
type corpusLine struct {
	url  string
	host string
}

// streamCorpus scans the CDX JSONL corpus a line at a time, decoding each into a
// corpusLine and handing it to fn, holding no more than one line plus the scan
// buffer in memory. It is the bounded intake the 100M ingest runs on: the corpus
// never becomes a resident slice, so the only resident growth is the store's own
// bounded structures. fn returning an error stops the scan and propagates it.
func streamCorpus(path string, fn func(corpusLine) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(bufio.NewReader(f))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec struct {
			URL  string `json:"url"`
			Host string `json:"host"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.URL == "" {
			continue
		}
		host := rec.Host
		if host == "" {
			host = frontier.HostOf(rec.URL)
		}
		if host == "" {
			continue
		}
		if err := fn(corpusLine{url: rec.URL, host: host}); err != nil {
			return err
		}
	}
	return sc.Err()
}

// readCorpus loads the CDX JSONL corpus into memory once so the seed and run
// stages measure the engine, not the JSON parser. At 10M lines this is a few GB,
// the same memory a fleet box holds the corpus in to seed a partition. The ingest
// stage does not call this; it uses streamCorpus to stay bounded at 100M.
func readCorpus(path string) ([]corpusLine, error) {
	var out []corpusLine
	err := streamCorpus(path, func(ln corpusLine) error {
		out = append(out, ln)
		return nil
	})
	return out, err
}

// profiledStage runs a stage under a CPU profile and writes a heap profile after,
// naming both <mode>.<stage>.<profile>.pprof so doc 05's cross-size comparison can
// diff the same stage across profile sizes. The stage's own timing and memory come
// from the harness inside fn; this only adds the profiler artifacts.
func profiledStage(dir, stage, tag string, fn func() (scale.StageResult, error)) (scale.StageResult, error) {
	cpuPath := filepath.Join(dir, fmt.Sprintf("cpu.%s.%s.pprof", stage, tag))
	cf, err := os.Create(cpuPath)
	if err != nil {
		return scale.StageResult{}, err
	}
	if err := pprof.StartCPUProfile(cf); err != nil {
		_ = cf.Close()
		return scale.StageResult{}, err
	}
	res, runErr := fn()
	pprof.StopCPUProfile()
	_ = cf.Close()
	if runErr != nil {
		return res, runErr
	}

	heapPath := filepath.Join(dir, fmt.Sprintf("heap.%s.%s.pprof", stage, tag))
	hf, err := os.Create(heapPath)
	if err != nil {
		return res, err
	}
	runtime.GC()
	if err := pprof.WriteHeapProfile(hf); err != nil {
		_ = hf.Close()
		return res, err
	}
	_ = hf.Close()
	return res, nil
}

// dirSize sums the byte size of every regular file directly under dir, the
// on-disk footprint of a store partition (its log, snapshot, and superblock). It
// is the bytes-on-disk denominator the ingest stage reports, measured by walking
// the directory rather than tracking writes so it counts exactly what landed.
func dirSize(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if info, err := e.Info(); err == nil {
			total += uint64(info.Size())
		}
	}
	return total
}

func shortCommit(c string) string {
	if len(c) > 7 {
		return c[:7]
	}
	if c == "" {
		return "nocommit"
	}
	return c
}
