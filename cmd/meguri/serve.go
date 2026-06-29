package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// newServeCmd opens a directory as a durable partition and drives its crawl loop,
// the long-running server form of `run` (doc 11, doc 04). Where `run` recovers a
// single .meguri file, drains it, and writes a fresh file, serve holds the
// log-structured store open for the life of the process: it recovers the resident
// frontier from the store, advances it through the engine, and folds it back to a
// durable checkpoint on shutdown, so a crash mid-crawl recovers from the log tail
// rather than losing the session. The production deployment binds the network
// fetcher (ami) and the fleet transport here; this command runs the same lifecycle
// with the offline drain fetcher so the open/advance/checkpoint/close path is
// exercised end to end on real data.
func newServeCmd() *cobra.Command {
	var (
		dir      string
		seed     string
		manifest string
		workers  int
		priority float64
		delay    uint16
		budget   int
		wall     bool
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Open a directory as a durable partition and drive its crawl loop",
		Long:  "serve opens --dir as a log-structured partition store, recovers its frontier (seeding from --seed CDX JSONL on a fresh directory), drives the staged engine loop to drain it with the offline fetcher, and checkpoints back on shutdown. --manifest reads a fleet catalog and reports where this partition's range routes. The production fetcher is ami, bound through the fetch.Fetcher SPI.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dir == "" {
				return fmt.Errorf("--dir is required")
			}

			p, err := engine.OpenPartition(dir,
				store.Options{ResidentBudget: budget},
				frontier.WithStateMachine())
			if err != nil {
				return fmt.Errorf("open partition %s: %w", dir, err)
			}
			fr := p.Frontier()

			if seed != "" {
				n, err := seedFromCDX(fr, seed, float32(priority), delay)
				if err != nil {
					_ = p.Abandon()
					return fmt.Errorf("seed %s: %w", seed, err)
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "seeded %d urls from %s\n", n, seed); err != nil {
					_ = p.Abandon()
					return err
				}
			}

			var clk engine.Clock
			if wall {
				clk = engine.WallClock{}
			} else {
				clk = engine.NewLogicalClock(uint32(time.Now().Unix()))
			}
			eng := engine.New(fr, engine.Config{
				Fetcher:    drainFetcher{},
				Workers:    workers,
				Clock:      clk,
				UntilEmpty: true,
			})

			before := fr.Len()
			if err := eng.Run(cmd.Context()); err != nil {
				_ = p.Abandon()
				return fmt.Errorf("run: %w", err)
			}
			st := eng.Stats()
			if _, err := fmt.Fprintf(cmd.OutOrStdout(),
				"served %d urls: %d dispatched, %d fetched, %d failed, %d pending\n",
				before, st.Dispatched, st.Fetched, st.Failed, fr.Pending()); err != nil {
				_ = p.Abandon()
				return err
			}

			if manifest != "" {
				if err := reportRoute(cmd, fr, manifest); err != nil {
					_ = p.Abandon()
					return err
				}
			}

			if err := p.Close(); err != nil {
				return fmt.Errorf("checkpoint and close: %w", err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "checkpointed %s\n", dir)
			return err
		},
	}
	cmd.Flags().StringVarP(&dir, "dir", "d", "", "partition store directory to open or create (required)")
	cmd.Flags().StringVar(&seed, "seed", "", "CDX JSONL seed list to load into a fresh partition")
	cmd.Flags().StringVar(&manifest, "manifest", "", "fleet manifest file to report this partition's routing against")
	cmd.Flags().IntVar(&workers, "workers", 0, "polite-host fetch parallelism (0 = default)")
	cmd.Flags().Float64Var(&priority, "priority", 0.5, "initial priority for seeded URLs")
	cmd.Flags().Uint16Var(&delay, "crawl-delay", 10, "default per-host crawl delay in deciseconds")
	cmd.Flags().IntVar(&budget, "resident-budget", 0, "max resident URL records (0 = unbounded)")
	cmd.Flags().BoolVar(&wall, "wall", false, "use a wall clock (real politeness waits) instead of the logical clock")
	return cmd
}

// reportRoute reads a fleet manifest and prints where this partition's host-key
// range lands in it: the entry that owns the range's low key and whether the
// manifest tiles the key space with no gap. It exercises the Manifest reader on a
// served partition, the routing question a live fleet answers per discovered link.
func reportRoute(cmd *cobra.Command, fr *frontier.Frontier, manifest string) error {
	col, err := engine.OpenCollection(manifest)
	if err != nil {
		return fmt.Errorf("open manifest %s: %w", manifest, err)
	}
	part := fr.Checkpoint()
	if e, ok := col.Route(part.HostKeyLo); ok {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(),
			"manifest: host-key 0x%016x routes to partition %d (%s)\n",
			part.HostKeyLo, e.PartitionID, e.FileRef); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(),
			"manifest: host-key 0x%016x has no owning partition\n", part.HostKeyLo); err != nil {
			return err
		}
	}
	if lo, hi, gap := col.Manifest().CoverageGap(0); gap {
		_, err = fmt.Fprintf(cmd.OutOrStdout(),
			"manifest: coverage gap [0x%016x, 0x%016x]\n", lo, hi)
		return err
	}
	_, err = fmt.Fprint(cmd.OutOrStdout(), "manifest: ranges tile the key space cleanly\n")
	return err
}

// seedFromCDX reads a CDX JSONL stream into a frontier, the shared seeding loop the
// serve and run paths use to load real URLs. It returns the count seeded so the
// caller can report it. A line that does not parse or carries no host is skipped,
// the same tolerant intake the seed command keeps.
func seedFromCDX(fr *frontier.Frontier, path string, priority float32, delay uint16) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(bufio.NewReader(f))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	n := 0
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec cdxLine
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
		fr.Seed(rec.URL, host, priority, 0, 0, delay)
		n++
	}
	if err := sc.Err(); err != nil {
		return n, fmt.Errorf("read seed: %w", err)
	}
	return n, nil
}
