package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/tamnd/meguri/live"
	"github.com/tamnd/meguri/scale"
	"github.com/tamnd/meguri/store"
)

// corpusSource pulls the CDX JSONL corpus one line at a time as live.Items for a
// bulk build. It is the streaming intake the file-backed engine loads 100M from:
// the corpus never becomes a resident slice, only the scan buffer plus the line in
// hand, so the build's residency is the loader's own bounded structures, not the
// corpus. Key construction matches the rest of the harness (host key from the
// host, path key from the canonical path).
type corpusSource struct {
	f   *os.File
	gz  *gzip.Reader
	sc  *bufio.Scanner
	cnt int
}

func newCorpusSource(path string) (*corpusSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var r io.Reader = bufio.NewReaderSize(f, 1<<20)
	var gz *gzip.Reader
	// A gzipped corpus streams through the decompressor so the 100M run never has
	// to spend the disk on an uncompressed copy; the box of record holds the corpus
	// as a 1.6 GB .gz, not a 10 GB .jsonl.
	if strings.HasSuffix(path, ".gz") {
		gz, err = gzip.NewReader(r)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		r = gz
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	return &corpusSource{f: f, gz: gz, sc: sc}, nil
}

func (s *corpusSource) Next() (live.Item, bool, error) {
	for s.sc.Scan() {
		line := strings.TrimSpace(s.sc.Text())
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
		s.cnt++
		key := meguri.URLKey{
			HostKey: meguri.HostKeyOf(host),
			PathKey: meguri.PathKeyOf(frontier.PathOf(rec.URL)),
		}
		return live.Item{Key: key, URL: rec.URL, Host: host}, true, nil
	}
	if err := s.sc.Err(); err != nil {
		return live.Item{}, false, err
	}
	return live.Item{}, false, nil
}

func (s *corpusSource) close() error {
	if s.gz != nil {
		_ = s.gz.Close()
	}
	return s.f.Close()
}

// buildLiveDelta streams the pinned corpus and fills a Stage 2 write buffer with a
// realistic mix of the two write kinds a live crawl produces: recrawl updates against
// keys that are in the base, and inserts of keys that are not. The first `updates`
// corpus rows become updates, their keys unchanged so they collide with the base and
// exercise the merge's tie path; each is stamped as if a recrawl just moved it to
// Crawled with the crawl counters bumped and the next fetch pushed a day out. The next
// `inserts` rows become discoveries, their PathKey perturbed by the same constant the
// dedup stage uses so the key is guaranteed absent from the base and the merge takes
// the insert path. Bounding the delta to updates+inserts keeps the resident write
// buffer a fixed fraction of the base, which is the residency claim the compaction
// stage measures.
func buildLiveDelta(input string, delta *live.Delta, updates, inserts int, nowHours uint32) error {
	src, err := newCorpusSource(input)
	if err != nil {
		return err
	}
	defer func() { _ = src.close() }()
	// Keep the insert keys off the base with the dedup stage's perturbation so the two
	// stages agree on what "a key the base does not have" means.
	const pathPerturb = 0x8000000000000001
	seen := 0
	for {
		it, ok, ne := src.Next()
		if ne != nil {
			return ne
		}
		if !ok {
			break
		}
		if seen < updates {
			// A recrawl update: same key, fresh crawl state a day out.
			delta.Put(live.DeltaEntry{
				Rec: meguri.URLRecord{
					URLKey:          it.Key,
					Status:          meguri.StatusCrawled,
					Priority:        0.5,
					FirstSeen:       nowHours,
					LastCrawled:     nowHours,
					NextDue:         nowHours + 24,
					CrawlCount:      1,
					DiscoverySource: meguri.SourceSeed,
				},
				URL:  it.URL,
				Host: it.Host,
			})
		} else if seen < updates+inserts {
			// A discovery: perturb the path so the key is absent from the base.
			key := it.Key
			key.PathKey ^= pathPerturb
			delta.Put(live.DeltaEntry{
				Rec: meguri.URLRecord{
					URLKey:          key,
					Status:          meguri.StatusScheduled,
					Priority:        0.5,
					DiscoverySource: meguri.SourceLink,
				},
				URL:  it.URL,
				Host: it.Host,
			})
		} else {
			break
		}
		seen++
	}
	return nil
}

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
		input             string
		profile           string
		box               string
		commit            string
		outDir            string
		doSeed            bool
		doRun             bool
		doInspect         bool
		doIngest          bool
		residentBudget    int
		seedMode          string
		streamCheckpoint  bool
		pageRows          int
		spillArena        bool
		arenaBudget       int64
		diskIndex         bool
		mergeBatch        int
		doCheckpoint      bool
		doLive            bool
		liveOpen          string
		liveExpect        uint64
		liveRunRows       int
		liveSample        int
		liveFP            float64
		liveCompact       bool
		liveCompactUpd    int
		liveCompactIns    int
		liveSchedule      bool
		liveScheduleBatch int
		liveRecrawl       bool
		liveRecrawlChange float64
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
					ingestLat      latStats
					ingestLatSum   *scale.LatencySummary
				)
				ingestStage, err := profiledStage(pprofDir, "ingest", tag, func() (scale.StageResult, error) {
					return scale.StageResultFromIngest(len(lines), func() (uint64, error) {
						st, e := store.Open(storeDir, store.Options{
							Durability:     store.DurabilityNormal,
							ResidentBudget: residentBudget,
							SpillArena:     spillArena,
							ArenaBudget:    arenaBudget,
							DiskIndex:      diskIndex,
							MergeBatch:     mergeBatch,
						})
						if e != nil {
							return 0, e
						}
						// Host dedup is the one resident structure ingest keeps: a set of
						// distinct host keys (millions, not the 100M URL count), so it stays
						// small. Everything per-URL spills to the store's log.
						seen := make(map[uint64]struct{}, 1<<16)
						ingestURLs = 0
						putLat := newLatHist()
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
							t0 := time.Now()
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
							putLat.observe(time.Since(t0))
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
						ingestLat = putLat.stats()
						ingestLatSum = putLat.summary("PutURL")
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
						if doCheckpoint {
							if streamCheckpoint {
								if ce := st.CheckpointStreaming(pageRows); ce != nil {
									return 0, ce
								}
							} else if ce := st.Checkpoint(); ce != nil {
								return 0, ce
							}
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
					"%s: %d resident of %d urls, held heap %.1f B/url, disk %.1f B/url, checkpoint total %.1f B/url, PutURL p50 %s p90 %s p99 %s max %s",
					budgetNote, ingestResident, ingestStage.URLs,
					float64(ingestHeld)/float64(max(ingestResident, 1)),
					float64(ingestDisk)/float64(max(ingestStage.URLs, 1)),
					float64(ingestSnap)/float64(max(ingestStage.URLs, 1)),
					ingestLat.p50, ingestLat.p90, ingestLat.p99, ingestLat.max)
				ingestStage.Latency = ingestLatSum
				result.Stages = append(result.Stages, ingestStage)
			}

			// Live stage: the clean-room file-backed engine of spec 2073 doc 08. It
			// is the single-file path the 100M goal runs on: one mmapped .meguri file
			// is intake, dedup, and lookup, with no DRUM, no append log, no spilled
			// arena. The build sub-stage streams the corpus through BulkLoad into one
			// compact file in bounded memory (external sort, host-clustered arena,
			// streaming columnar encode). The lookup sub-stage maps the file back and
			// replays the corpus as a dedup pass, so the resident cost is the blocked
			// Bloom filter while the multi-gigabyte base stays reclaimable page cache.
			// The anon/file RSS split is the metric of record: anon is the budget the
			// box caps, file is the mapped base the kernel reclaims first.
			if doLive {
				livePath := filepath.Join(outDir, fmt.Sprintf("%s.live.meguri", tag))
				var (
					buildRes live.BuildResult
					buildRSS scale.RSSSplit
				)
				// --live-open points the read passes at an existing .meguri and skips the
				// build sub-stage, so dedup, rediscover, and schedule replay a prebuilt
				// store without a 35-minute rebuild or its scratch. The build's only
				// downstream input is the file's byte size (dedup and compact stamp it as
				// BytesRead), so a Stat is all the later stages need from a skipped build.
				if liveOpen != "" {
					livePath = liveOpen
					fi, se := os.Stat(livePath)
					if se != nil {
						return fmt.Errorf("live-open %q: %w", liveOpen, se)
					}
					buildRes.FileBytes = fi.Size()
				} else {
					if err := os.Remove(livePath); err != nil && !os.IsNotExist(err) {
						return err
					}
					buildStage, err := profiledStage(pprofDir, "live-build", tag, func() (scale.StageResult, error) {
						return scale.StageResultFromLive(0, func() (uint64, error) {
							src, e := newCorpusSource(input)
							if e != nil {
								return 0, e
							}
							r, e := live.BulkLoad(src, live.BuildOptions{
								Path:         livePath,
								TmpDir:       outDir,
								ExpectedKeys: liveExpect,
								RunRows:      liveRunRows,
								PageRows:     pageRows,
								FPRate:       liveFP,
								Codec:        format.CodecZstd,
								NowHours:     uint32(time.Now().Unix() / 3600),
								Status:       meguri.StatusScheduled,
								Source:       meguri.SourceSeed,
							})
							if e != nil {
								_ = src.close()
								return 0, e
							}
							_ = src.close()
							buildRes = r
							buildRSS = scale.ReadRSSSplit()
							return uint64(r.FileBytes), nil
						})
					})
					if err != nil {
						return fmt.Errorf("live-build stage: %w", err)
					}
					buildStage = scale.WithURLs(buildStage, buildRes.URLCount)
					buildStage.RSS = buildRSS
					buildStage.Notes = fmt.Sprintf(
						"%d urls, %d hosts, file %.2f B/url, filter %.2f bits/url, anon %s after build",
						buildRes.URLCount, buildRes.HostCount,
						float64(buildRes.FileBytes)/float64(max(buildRes.URLCount, 1)),
						buildRes.BitsPerURL, humanRSSBytes(buildRSS.AnonBytes))
					result.Stages = append(result.Stages, buildStage)
				}

				// Dedup sub-stage: map the file back and replay the corpus as a stream
				// of fresh discoveries, the common intake case. Each key is perturbed so
				// it is absent from the base, so the resident filter answers "new"
				// without faulting a file page; only the filter's false positives fall
				// through to the cheap base presence check. This is the throughput that
				// matters at 100M: intake against a large base stays resident, the
				// multi-gigabyte file untouched. The RSS split after the pass is the
				// proof: anon is the filter, file stays near zero because the fast path
				// never decodes a page. The handful of confirmed hits are false
				// positives (a perturbed key is never really present).
				var (
					dedupURLs   int
					dedupBase   uint64
					dedupHit    int
					dedupLat    latStats
					dedupLatSum *scale.LatencySummary
					dedupRate   float64
					dedupRSS    scale.RSSSplit
					dedupBPU    float64
				)
				const pathPerturb = 0x8000000000000001
				dedupStage, err := profiledStage(pprofDir, "live-dedup", tag, func() (scale.StageResult, error) {
					return scale.StageResultFromLive(0, func() (uint64, error) {
						eng, e := live.Open(livePath)
						if e != nil {
							return 0, e
						}
						defer func() { _ = eng.Close() }()
						dedupBPU = eng.BitsPerURL()
						src, e := newCorpusSource(input)
						if e != nil {
							return 0, e
						}
						defer func() { _ = src.close() }()
						lat := newLatHist()
						for {
							it, ok, ne := src.Next()
							if ne != nil {
								return 0, ne
							}
							if !ok {
								break
							}
							dedupURLs++
							probe := it.Key
							probe.PathKey ^= pathPerturb
							t0 := time.Now()
							hit, se := eng.Seen(probe)
							if se != nil {
								return 0, se
							}
							lat.observe(time.Since(t0))
							if hit {
								dedupHit++ // base confirmed a perturbed key present (vanishingly rare)
							}
						}
						dedupLat = lat.stats()
						dedupLatSum = lat.summary("Seen")
						dedupRate = lat.engineRate()
						dedupBase = eng.BaseProbes()
						dedupRSS = scale.ReadRSSSplit()
						return uint64(buildRes.FileBytes), nil
					})
				})
				if err != nil {
					return fmt.Errorf("live-dedup stage: %w", err)
				}
				dedupStage = scale.WithURLs(dedupStage, dedupURLs)
				dedupStage.RSS = dedupRSS
				dedupStage.Notes = fmt.Sprintf(
					"%d probes, %d filter-miss (resident, no file), %d base-confirm (%.3f%% FP), %d present, filter %.2f bits/url, %s, Seen p50 %s p99 %s engine %s urls/s",
					dedupURLs, dedupURLs-int(dedupBase), dedupBase,
					100*float64(dedupBase)/float64(max(dedupURLs, 1)), dedupHit, dedupBPU,
					rssNote(dedupRSS), dedupLat.p50, dedupLat.p99, humanCount(dedupRate))
				dedupStage.Latency = dedupLatSum
				result.Stages = append(result.Stages, dedupStage)

				// Rediscover sub-stage: the minority path, a rediscovery that hits the
				// base. It samples present keys and looks each one up, so every probe is
				// a filter hit confirmed against the mapped file, the cost of a true
				// rediscovery (one zone-pruned key-column page decode). It is sampled,
				// not run over the whole corpus, because the slow path's per-op cost is
				// what we characterize, not its aggregate (a real stream hits it rarely).
				var (
					rediscSeen   int
					rediscN      int
					rediscLat    latStats
					rediscLatSum *scale.LatencySummary
					rediscRate   float64
					rediscRSS    scale.RSSSplit
				)
				rediscStage, err := profiledStage(pprofDir, "live-rediscover", tag, func() (scale.StageResult, error) {
					return scale.StageResultFromLive(0, func() (uint64, error) {
						eng, e := live.Open(livePath)
						if e != nil {
							return 0, e
						}
						defer func() { _ = eng.Close() }()
						src, e := newCorpusSource(input)
						if e != nil {
							return 0, e
						}
						defer func() { _ = src.close() }()
						// Sample every stride-th key up to the sample cap, so the probes
						// are spread across the whole host-clustered key space rather than
						// one contiguous page.
						stride := max(buildRes.URLCount/liveSample, 1)
						lat := newLatHist()
						idx := 0
						for rediscN < liveSample {
							it, ok, ne := src.Next()
							if ne != nil {
								return 0, ne
							}
							if !ok {
								break
							}
							if idx%stride != 0 {
								idx++
								continue
							}
							idx++
							rediscN++
							t0 := time.Now()
							hit, se := eng.Seen(it.Key)
							if se != nil {
								return 0, se
							}
							lat.observe(time.Since(t0))
							if hit {
								rediscSeen++
							}
						}
						rediscLat = lat.stats()
						rediscLatSum = lat.summary("Seen")
						rediscRate = lat.engineRate()
						rediscRSS = scale.ReadRSSSplit()
						return uint64(buildRes.FileBytes), nil
					})
				})
				if err != nil {
					return fmt.Errorf("live-rediscover stage: %w", err)
				}
				rediscStage = scale.WithURLs(rediscStage, rediscN)
				rediscStage.RSS = rediscRSS
				rediscStage.Disk = scale.DiskSummary{BytesRead: uint64(buildRes.FileBytes)}
				rediscStage.Notes = fmt.Sprintf(
					"%d/%d sampled keys confirmed off mapped file (cache %d pages), %s, Seen p50 %s p90 %s p99 %s max %s, engine %s urls/s",
					rediscSeen, rediscN, 64,
					rssNote(rediscRSS),
					rediscLat.p50, rediscLat.p90, rediscLat.p99, rediscLat.max, humanCount(rediscRate))
				rediscStage.Latency = rediscLatSum
				result.Stages = append(result.Stages, rediscStage)

				// Compact sub-stage: the Stage 2 write path. Buffer a bounded delta of
				// recrawl updates (existing keys re-fetched with fresh crawl state) and
				// inserts (new discoveries), then fold it into the base file to produce
				// the next generation. The read side is the cursor-based merge the
				// rediscovery tail argued for: the base URL table is walked once in key
				// order, not probed at random, so the arena resolves sequentially with
				// one blob page resident. That is the property the doc claims and this
				// stage measures at 100M: the base rewrites within the box budget, the
				// anon term bounded by the delta plus host table plus filter while the
				// base and output stream through as reclaimable page cache.
				if liveCompact {
					nowHours := uint32(time.Now().Unix() / 3600)
					delta := live.NewDelta()
					deltaT0 := time.Now()
					if e := buildLiveDelta(input, delta, liveCompactUpd, liveCompactIns, nowHours); e != nil {
						return fmt.Errorf("live-compact delta build: %w", e)
					}
					deltaWall := time.Since(deltaT0)
					gen2Path := filepath.Join(outDir, fmt.Sprintf("%s.gen2.meguri", tag))
					if err := os.Remove(gen2Path); err != nil && !os.IsNotExist(err) {
						return err
					}
					var (
						compRes live.CompactResult
						compRSS scale.RSSSplit
					)
					compactStage, err := profiledStage(pprofDir, "live-compact", tag, func() (scale.StageResult, error) {
						return scale.StageResultFromLive(0, func() (uint64, error) {
							r, e := live.Compact(livePath, delta, live.CompactOptions{
								OutPath:  gen2Path,
								TmpDir:   outDir,
								PageRows: pageRows,
								Codec:    format.CodecZstd,
								FPRate:   liveFP,
								NowHours: nowHours,
							})
							if e != nil {
								return 0, e
							}
							compRes = r
							compRSS = scale.ReadRSSSplit()
							return uint64(r.FileBytes), nil
						})
					})
					if err != nil {
						return fmt.Errorf("live-compact stage: %w", err)
					}
					compactStage = scale.WithURLs(compactStage, compRes.URLCount)
					compactStage.RSS = compRSS
					compactStage.Disk = scale.DiskSummary{
						BytesRead:    uint64(buildRes.FileBytes),
						BytesWritten: uint64(compRes.FileBytes),
						OutputBytes:  uint64(compRes.FileBytes),
					}
					compactStage.Notes = fmt.Sprintf(
						"%d urls out (%d carried, %d updated, %d inserted), %d hosts, file %.2f B/url, filter %.2f bits/url, delta %d entries built in %s, %s",
						compRes.URLCount, compRes.Carried, compRes.Updated, compRes.Inserted,
						compRes.HostCount, float64(compRes.FileBytes)/float64(max(compRes.URLCount, 1)),
						compRes.BitsPerURL, delta.Len(), deltaWall.Round(time.Millisecond),
						rssNote(compRSS))
					result.Stages = append(result.Stages, compactStage)
				}

				// Schedule sub-stage: the Stage 3 read, "find the next URL to
				// dispatch". It opens the newest generation (the gen2 file when a
				// compaction ran, else the base) and drains the due set off the mapped
				// file with the bounded DueCursor, so a scheduler pulls work in capped
				// batches instead of materializing the whole next_due column. The now is
				// set past every scheduled due time, so the whole backlog is due and no
				// page is pruned by the future-zone pushdown: this is the heaviest
				// dispatch scan, the one to size the box against. The residency proof is
				// the same as the write path: the scan reads next_due and key pages in
				// stored order, so the base streams through as reclaimable cache rather
				// than a resident working set.
				if liveSchedule {
					schedPath := livePath
					if liveCompact {
						schedPath = filepath.Join(outDir, fmt.Sprintf("%s.gen2.meguri", tag))
					}
					var (
						schedDispatched int
						schedBatches    int
						schedLat        latStats
						schedLatSum     *scale.LatencySummary
						schedRate       float64
						schedRSS        scale.RSSSplit
						schedBytes      int64
					)
					scheduleStage, err := profiledStage(pprofDir, "live-schedule", tag, func() (scale.StageResult, error) {
						return scale.StageResultFromLive(0, func() (uint64, error) {
							if fi, se := os.Stat(schedPath); se == nil {
								schedBytes = fi.Size()
							}
							eng, e := live.Open(schedPath)
							if e != nil {
								return 0, e
							}
							defer func() { _ = eng.Close() }()
							cur, e := eng.DueCursor(^uint32(0))
							if e != nil {
								return 0, e
							}
							lat := newLatHist()
							drainT0 := time.Now()
							for {
								t0 := time.Now()
								batch, be := cur.NextBatch(liveScheduleBatch)
								if be != nil {
									return 0, be
								}
								if batch == nil {
									break
								}
								lat.observe(time.Since(t0))
								schedDispatched += len(batch)
								schedBatches++
							}
							drainWall := time.Since(drainT0)
							schedLat = lat.stats()
							schedLatSum = lat.summary("NextBatch")
							if drainWall > 0 {
								schedRate = float64(schedDispatched) / drainWall.Seconds()
							}
							schedRSS = scale.ReadRSSSplit()
							return uint64(schedBytes), nil
						})
					})
					if err != nil {
						return fmt.Errorf("live-schedule stage: %w", err)
					}
					scheduleStage = scale.WithURLs(scheduleStage, schedDispatched)
					scheduleStage.RSS = schedRSS
					scheduleStage.Disk = scale.DiskSummary{BytesRead: uint64(schedBytes)}
					scheduleStage.Notes = fmt.Sprintf(
						"%d urls dispatched off the mapped file in %d batches of %d, %s, NextBatch p50 %s p90 %s p99 %s max %s, dispatch %s urls/s",
						schedDispatched, schedBatches, liveScheduleBatch,
						rssNote(schedRSS),
						schedLat.p50, schedLat.p90, schedLat.p99, schedLat.max, humanCount(schedRate))
					scheduleStage.Latency = schedLatSum
					result.Stages = append(result.Stages, scheduleStage)
				}

				// Recrawl sub-stage: the Stage 3 write, "update a URL after it is
				// fetched". It reads the newest generation (gen2 after a compaction, else
				// the base), streams every row in stored key order, and for a row that is
				// due folds a typed crawl outcome into its change-rate counters, re-
				// estimates the Poisson rate, and reschedules it, writing the result as a
				// new generation with an atomic swap. The now is set past every due time,
				// so the whole frontier is folded: the heaviest recrawl, the write twin of
				// the schedule drain. The read is the sequential cursor the compaction
				// uses, so the base streams through as reclaimable cache and the outcome
				// fold never pays the random point-lookup tail a per-key GetURL would.
				if liveRecrawl {
					recrawlIn := livePath
					if liveCompact {
						recrawlIn = filepath.Join(outDir, fmt.Sprintf("%s.gen2.meguri", tag))
					}
					gen3Path := filepath.Join(outDir, fmt.Sprintf("%s.gen3.meguri", tag))
					if err := os.Remove(gen3Path); err != nil && !os.IsNotExist(err) {
						return fmt.Errorf("clear gen3: %w", err)
					}
					nowHours := uint32(time.Now().Unix() / 3600)
					var (
						recRes  live.RecrawlResult
						recRSS  scale.RSSSplit
						recIn   int64
						recRate float64
					)
					recrawlStage, err := profiledStage(pprofDir, "live-recrawl", tag, func() (scale.StageResult, error) {
						return scale.StageResultFromLive(0, func() (uint64, error) {
							if fi, se := os.Stat(recrawlIn); se == nil {
								recIn = fi.Size()
							}
							t0 := time.Now()
							r, e := live.Recrawl(recrawlIn, live.RecrawlOptions{
								OutPath:    gen3Path,
								TmpDir:     outDir,
								PageRows:   pageRows,
								Codec:      format.CodecZstd,
								NowHours:   nowHours,
								FPRate:     liveFP,
								Tau:        1e-4,
								ChangeRate: liveRecrawlChange,
								Seed:       1,
							})
							if e != nil {
								return 0, e
							}
							wall := time.Since(t0)
							if wall > 0 {
								recRate = float64(r.Recrawled) / wall.Seconds()
							}
							recRes = r
							recRSS = scale.ReadRSSSplit()
							return uint64(r.FileBytes), nil
						})
					})
					if err != nil {
						return fmt.Errorf("live-recrawl stage: %w", err)
					}
					recrawlStage = scale.WithURLs(recrawlStage, recRes.URLCount)
					recrawlStage.RSS = recRSS
					recrawlStage.Disk = scale.DiskSummary{
						BytesRead:    uint64(recIn),
						BytesWritten: uint64(recRes.FileBytes),
						OutputBytes:  uint64(recRes.FileBytes),
					}
					recrawlStage.Notes = fmt.Sprintf(
						"%d urls out (%d recrawled, %d carried, %d changed, %d no-change), %d hosts, file %.2f B/url, filter %.2f bits/url, mean lambda %.3e/hr, %s, recrawl %s urls/s",
						recRes.URLCount, recRes.Recrawled, recRes.Carried, recRes.Changed, recRes.NoChange,
						recRes.HostCount, float64(recRes.FileBytes)/float64(max(recRes.URLCount, 1)), recRes.BitsPerURL,
						recRes.MeanLambda, rssNote(recRSS), humanCount(recRate))
					result.Stages = append(result.Stages, recrawlStage)
				}
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
	cmd.Flags().BoolVar(&diskIndex, "disk-index", false, "hold the URL seen-set and location index on disk in the DRUM (spec 2072 Stage B), removing the ~80-90 B/url resident index term")
	cmd.Flags().IntVar(&mergeBatch, "merge-batch", 0, "discoveries to accumulate before folding into the DRUM repository (0 picks the 2M default); smaller batches merge more often for less in-flight RAM")
	cmd.Flags().BoolVar(&doCheckpoint, "checkpoint", true, "write the durable checkpoint after ingest; set false to measure pure ingest residency without the snapshot encode (the durable log+drum+arena store survives without the .meguri export)")
	cmd.Flags().BoolVar(&doLive, "live", false, "drive the clean-room file-backed engine (spec 2073 doc 08): bulk-load the corpus into one mmapped .meguri file, then replay it as a dedup pass, capturing the anon/file RSS split")
	cmd.Flags().StringVar(&liveOpen, "live-open", "", "open an existing .meguri for the live read passes instead of building one; skips the build sub-stage so dedup, rediscover, and schedule replay a prebuilt store without a rebuild")
	cmd.Flags().Uint64Var(&liveExpect, "live-expect", 0, "expected distinct URL count for the live build (sizes the resident filter; 0 picks 1M, pass the real corpus size for a 100M run)")
	cmd.Flags().IntVar(&liveRunRows, "live-run-rows", 0, "external-sort buffer cap in rows for the live build (0 picks 1M, the bounded-memory sort window)")
	cmd.Flags().IntVar(&liveSample, "live-sample", 100000, "present-key sample size for the live-rediscover stage (the base point-lookup latency is characterized on a sample, not the whole corpus)")
	cmd.Flags().Float64Var(&liveFP, "live-fp", 0, "resident filter false-positive rate for the live build (0 picks 1%); a lower rate keeps base-confirmations rare at 100M, trading a few more bits/url of resident filter")
	cmd.Flags().BoolVar(&liveCompact, "live-compact", false, "run the Stage 2 write path (spec 2073 doc 08): buffer a bounded delta of recrawl updates and inserts, then compact it into the base .meguri to produce the next file generation with an atomic swap")
	cmd.Flags().IntVar(&liveCompactUpd, "live-compact-updates", 1000000, "number of recrawl updates to buffer in the delta before compacting (existing keys re-fetched with fresh crawl state)")
	cmd.Flags().IntVar(&liveCompactIns, "live-compact-inserts", 250000, "number of new keys to insert through the delta (discoveries not in the base)")
	cmd.Flags().BoolVar(&liveSchedule, "live-schedule", false, "run the Stage 3 scheduler read (spec 2073 doc 08): drain the due set off the newest generation with the bounded DueCursor, capturing dispatch throughput and the anon/file RSS split")
	cmd.Flags().IntVar(&liveScheduleBatch, "live-schedule-batch", 10000, "due-key batch cap for the schedule dispatch scan (the scheduler pulls work in capped batches, not the whole due set at once)")
	cmd.Flags().BoolVar(&liveRecrawl, "live-recrawl", false, "run the Stage 3 recrawl write (spec 2073 doc 08): fold a typed crawl outcome into every due row of the newest generation, re-estimate its change rate, and reschedule it into a new generation with an atomic swap")
	cmd.Flags().Float64Var(&liveRecrawlChange, "live-recrawl-change", 0.2, "probability a folded outcome is a real content change (the rest are 304 no-change), the synthetic outcome stream's change rate")
	return cmd
}

// humanRSSBytes renders a byte count in MiB for the live stage notes, the unit the
// anon/file residency split reads in (a 100M filter is hundreds of MiB, the mapped
// base is gigabytes).
func humanRSSBytes(b uint64) string {
	return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
}

// humanCount renders a rate or count with a K/M/B suffix for the live stage notes.
func humanCount(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.2fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

// rssNote renders the anon/file residency split for a stage note, or "n/a" off
// Linux where /proc/self/status (the only source of the split) does not exist. The
// runs of record are on Linux, so this populates there; on the darwin dev box it
// keeps the note honest rather than printing a misleading zero split.
func rssNote(rss scale.RSSSplit) string {
	if !rss.Available {
		return "rss split n/a (non-Linux box)"
	}
	return fmt.Sprintf("anon %s file %s", humanRSSBytes(rss.AnonBytes), humanRSSBytes(rss.FileBytes))
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

// dirSize sums the byte size of every regular file under dir, recursing into
// subdirectories, the full on-disk footprint of a store partition: its log and
// superblock directly under dir, plus the DRUM repository and block index under
// dir/drum. It walks the tree rather than tracking writes so it counts exactly
// what landed, including the disk-index repository the earlier flat scan missed.
func dirSize(dir string) uint64 {
	var total uint64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

// latHist is a coarse log2-bucketed latency histogram for the PutURL hot path. It
// holds one uint64 counter per power-of-two nanosecond bucket (bucket i counts
// samples in [2^i, 2^(i+1)) ns), so observing a sample is one Leading-zeros and one
// increment, no allocation and no lock, cheap enough to wrap every PutURL in a
// 100M ingest. The percentile read walks the 64 buckets once and reports the
// bucket's upper edge, so the figures are order-of-magnitude exact, which is all a
// per-op latency at hundreds-of-nanoseconds scale needs to characterize the path.
type latHist struct {
	buckets [64]uint64
	count   uint64
	totalNs uint64
}

func newLatHist() *latHist { return &latHist{} }

func (h *latHist) observe(d time.Duration) {
	ns := uint64(d)
	if ns == 0 {
		ns = 1
	}
	h.totalNs += ns
	b := 63
	for ns>>b == 0 {
		b--
	}
	h.buckets[b]++
	h.count++
}

// engineRate is the throughput of the measured op alone, count over the summed
// observed durations, isolating the engine from the corpus parse the stage wall
// also includes. It is the number that says how fast the dedup decision itself is,
// independent of how fast the JSONL feeding it parses.
func (h *latHist) engineRate() float64 {
	if h.totalNs == 0 {
		return 0
	}
	return float64(h.count) / (float64(h.totalNs) / 1e9)
}

// latStats is the rendered percentile summary the ingest notes print.
type latStats struct {
	p50, p90, p99, max time.Duration
}

func (h *latHist) stats() latStats {
	if h.count == 0 {
		return latStats{}
	}
	edge := func(b int) time.Duration { return time.Duration(uint64(1) << uint(b+1)) }
	pick := func(target uint64) time.Duration {
		var cum uint64
		for b := range 64 {
			cum += h.buckets[b]
			if cum >= target {
				return edge(b)
			}
		}
		return 0
	}
	var maxB int
	for b := 63; b >= 0; b-- {
		if h.buckets[b] > 0 {
			maxB = b
			break
		}
	}
	return latStats{
		p50: pick((h.count*50 + 99) / 100),
		p90: pick((h.count*90 + 99) / 100),
		p99: pick((h.count*99 + 99) / 100),
		max: edge(maxB),
	}
}

// summary renders the histogram into the structured scale.LatencySummary the ledger
// carries. It tags the figures with the op name so a reader knows which hot path they
// measure, and pairs the percentile edges with the sample count and the engine-only
// rate. It is the machine-readable twin of the p50/p90/p99 the stage Notes print.
func (h *latHist) summary(op string) *scale.LatencySummary {
	s := h.stats()
	return &scale.LatencySummary{
		Op:            op,
		Samples:       h.count,
		P50Ns:         uint64(s.p50),
		P90Ns:         uint64(s.p90),
		P99Ns:         uint64(s.p99),
		MaxNs:         uint64(s.max),
		EngineOpsPerS: h.engineRate(),
	}
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
