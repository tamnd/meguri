package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/bench"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
)

// newBenchCmd assembles the deterministic per-partition costs of doc 14 on a real
// corpus slice and prints the hundred-billion projection. It reads the same CDX
// JSONL the seed path does, builds a real partition with its URL strings, counts
// the .meguri bytes per URL and the seen-set bits per URL with the achieved
// false-positive rate, and multiplies each out to the fleet totals at a stated
// total and per-partition capacity. The latency and throughput numbers stay in
// the go test -bench micro-benchmarks; this command owns only the counts and the
// projection that rests on them (doc 14, sections 3.7, 3.8, 6).
func newBenchCmd() *cobra.Command {
	var (
		input      string
		totalURLs  float64
		perPart    float64
		priority   float64
		crawlDelay uint16
		selRate    float64
	)
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Measure per-partition costs on a corpus slice and project to 100B URLs",
		Long:  "bench reads Common Crawl CDX records (ccrawl search ... -o jsonl) from --input or stdin, builds a real partition, measures the deterministic .meguri bytes/url and seen-set bits/url with its achieved fp rate, and prints the fleet projection as measured-times-count against the three named walls.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			in := cmd.InOrStdin()
			if input != "" {
				f, err := os.Open(input)
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				in = f
			}

			fr := frontier.New(0, 0)
			skipped := 0
			sc := bufio.NewScanner(bufio.NewReader(in))
			sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if line == "" {
					continue
				}
				var rec cdxLine
				if json.Unmarshal([]byte(line), &rec) != nil || rec.URL == "" {
					skipped++
					continue
				}
				host := rec.Host
				if host == "" {
					host = frontier.HostOf(rec.URL)
				}
				if host == "" {
					skipped++
					continue
				}
				fr.Seed(rec.URL, host, float32(priority), 0, 0, crawlDelay)
			}
			if err := sc.Err(); err != nil {
				return fmt.Errorf("read input: %w", err)
			}
			if fr.Len() == 0 {
				return fmt.Errorf("no urls read (%d lines skipped)", skipped)
			}

			// Round-trip through the on-disk format so the measured partition is the
			// real .meguri file, strings and all, not an in-memory shortcut.
			raw, err := fr.CheckpointBytes()
			if err != nil {
				return fmt.Errorf("encode checkpoint: %w", err)
			}
			part, err := format.Decode(raw)
			if err != nil {
				return fmt.Errorf("decode checkpoint: %w", err)
			}

			meas, err := bench.Measure(part)
			if err != nil {
				return fmt.Errorf("measure: %w", err)
			}
			proj := bench.Project(meas, totalURLs, perPart)
			walls := bench.Walls(part)

			if _, err = fmt.Fprint(cmd.OutOrStdout(), bench.Report(meas, proj, walls)); err != nil {
				return err
			}
			thr := bench.Analyze(part, selRate)
			_, err = fmt.Fprint(cmd.OutOrStdout(), "\n"+bench.ThroughputReport(thr))
			return err
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "CDX JSONL file to read (default stdin)")
	cmd.Flags().Float64Var(&totalURLs, "total-urls", 100e9, "fleet total URL count to project to")
	cmd.Flags().Float64Var(&perPart, "urls-per-partition", 30e6, "per-partition capacity, the projection lever")
	cmd.Flags().Float64Var(&priority, "priority", 0.5, "initial priority for every seeded URL")
	cmd.Flags().Uint16Var(&crawlDelay, "crawl-delay", 10, "default per-host crawl delay in deciseconds")
	cmd.Flags().Float64Var(&selRate, "scheduler-sel-rate", 1e6, "measured scheduler selections/s (from BenchmarkCorpusDispatchSelections) to report the politeness ceiling against")
	return cmd
}
