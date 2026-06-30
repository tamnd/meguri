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
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/scale"
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
		input    string
		profile  string
		box      string
		commit   string
		outDir   string
		doRun    bool
		seedMode string
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

			lines, err := readCorpus(input)
			if err != nil {
				return err
			}
			if len(lines) == 0 {
				return fmt.Errorf("corpus %s is empty", input)
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
			var seeded *frontier.Frontier
			seedStage, err := profiledStage(pprofDir, "seed", tag, func() (scale.StageResult, error) {
				return scale.StageResultFromSeed(len(lines), func() (uint64, error) {
					fr := frontier.New(1, 0)
					seedInto(fr)
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
			seedStage.Notes = fmt.Sprintf("%d urls resident after dedup", seeded.Len())
			result.Stages = append(result.Stages, seedStage)

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
	cmd.Flags().BoolVar(&doRun, "run", true, "drive the run stage (engine drain) in addition to seed")
	cmd.Flags().StringVar(&seedMode, "seed-mode", "batch", "seed intake path: batch (DRUM merge, the default) or loop (per-key, the pre-fix baseline)")
	return cmd
}

// corpusLine is the one capture the scale runner seeds from.
type corpusLine struct {
	url  string
	host string
}

// readCorpus loads the CDX JSONL corpus into memory once so the seed and run
// stages measure the engine, not the JSON parser. At 10M lines this is a few GB,
// the same memory a fleet box holds the corpus in to seed a partition.
func readCorpus(path string) ([]corpusLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var out []corpusLine
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
		out = append(out, corpusLine{url: rec.URL, host: host})
	}
	return out, sc.Err()
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

func shortCommit(c string) string {
	if len(c) > 7 {
		return c[:7]
	}
	if c == "" {
		return "nocommit"
	}
	return c
}
