package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
)

// drainFetcher is the offline fetcher the CLI binds when there is no network
// fetcher: it marks each dispatched URL crawled with a 200 and the current
// epoch-hour, extracting no links and reading no body. It turns `meguri run` into
// a scheduler drive, draining the frontier in exact priority-then-politeness order
// so the loop, the politeness floor, and the checkpoint are exercised end to end
// on real data. The production fetcher is ami, bound through the same
// fetch.Fetcher SPI; nothing else in the engine changes between them.
type drainFetcher struct{}

func (drainFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	return meguri.Outcome{
		URLKey:     req.URLKey,
		HTTPStatus: 200,
		FetchedAt:  uint32(time.Now().Unix() / 3600),
	}, nil
}

// newRunCmd drives the frontier engine over a checkpoint or a seed list. It builds
// the partition (recovering a .meguri file or seeding a fresh frontier from CDX
// JSONL), runs the staged engine loop with the offline drain fetcher under a
// logical clock so politeness waits collapse, reports what moved, and optionally
// writes the post-run checkpoint. It is the M3 end-to-end path: the same loop a
// live crawl runs, with the network fetcher swapped for the offline one.
func newRunCmd() *cobra.Command {
	var (
		input    string
		seed     string
		out      string
		workers  int
		priority float64
		delay    uint16
		wall     bool
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Drive the frontier engine over a checkpoint or seed list",
		Long:  "run loads a partition (--input .meguri or --seed CDX JSONL), drives the staged engine loop to drain it in priority-then-politeness order with the offline fetcher, and writes the result to --out. The production fetcher is ami, bound through the fetch.Fetcher SPI.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if input == "" && seed == "" {
				return fmt.Errorf("one of --input or --seed is required")
			}
			if input != "" && seed != "" {
				return fmt.Errorf("--input and --seed are mutually exclusive")
			}

			fr, err := buildFrontier(input, seed, float32(priority), delay)
			if err != nil {
				return err
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
				return fmt.Errorf("run: %w", err)
			}
			st := eng.Stats()
			if _, err := fmt.Fprintf(cmd.OutOrStdout(),
				"ran %d urls: %d dispatched, %d fetched, %d failed, %d pending\n",
				before, st.Dispatched, st.Fetched, st.Failed, fr.Pending()); err != nil {
				return err
			}

			if out == "" {
				return nil
			}
			raw, err := fr.CheckpointBytes()
			if err != nil {
				return fmt.Errorf("encode checkpoint: %w", err)
			}
			if err := os.WriteFile(out, raw, 0o644); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "wrote %s, %d bytes\n", out, len(raw))
			return err
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", ".meguri checkpoint to recover and run")
	cmd.Flags().StringVar(&seed, "seed", "", "CDX JSONL seed list to run a fresh frontier from")
	cmd.Flags().StringVarP(&out, "out", "o", "", "path to write the post-run .meguri checkpoint")
	cmd.Flags().IntVar(&workers, "workers", 0, "polite-host fetch parallelism (0 = default)")
	cmd.Flags().Float64Var(&priority, "priority", 0.5, "initial priority for seeded URLs")
	cmd.Flags().Uint16Var(&delay, "crawl-delay", 10, "default per-host crawl delay in deciseconds")
	cmd.Flags().BoolVar(&wall, "wall", false, "use a wall clock (real politeness waits) instead of the logical clock")
	return cmd
}

// buildFrontier recovers a frontier from a .meguri checkpoint or seeds a fresh one
// from a CDX JSONL list, the two ways `meguri run` starts a partition.
func buildFrontier(input, seed string, priority float32, delay uint16) (*frontier.Frontier, error) {
	if input != "" {
		raw, err := os.ReadFile(input)
		if err != nil {
			return nil, err
		}
		part, err := format.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", input, err)
		}
		return frontier.Recover(part), nil
	}

	f, err := os.Open(seed)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	fr := frontier.New(1, 0)
	sc := bufio.NewScanner(bufio.NewReader(f))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
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
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read seed: %w", err)
	}
	return fr, nil
}
