package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/frontier"
)

// cdxLine is one Common Crawl capture as ccrawl-cli emits it with `-o jsonl`.
// Only the fields the frontier seeds from are decoded; the rest are ignored.
type cdxLine struct {
	URL       string `json:"url"`
	Host      string `json:"host"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// newSeedCmd reads a CDX JSONL stream of URLs (the output of `ccrawl search ...
// -o jsonl`) into a fresh frontier and writes it out as a .meguri checkpoint. It
// is the M1 end-to-end path on real data: real URLs become frontier entries with
// their host politeness state, and the result is a file `inspect` can read and a
// later run can recover. Priority and freshness scheduling are still flat here;
// the OPIC and Poisson models that set them land in M4 and M5.
func newSeedCmd() *cobra.Command {
	var (
		input    string
		out      string
		priority float64
		delay    uint16
	)
	cmd := &cobra.Command{
		Use:   "seed",
		Short: "Build a .meguri checkpoint from a CDX JSONL list of URLs",
		Long:  "seed reads Common Crawl CDX records (ccrawl search ... -o jsonl) from --input or stdin, inserts each URL into a fresh frontier, and writes a .meguri checkpoint to --out.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if out == "" {
				return fmt.Errorf("--out is required")
			}
			in := cmd.InOrStdin()
			if input != "" {
				f, err := os.Open(input)
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				in = f
			}

			fr := frontier.New(1, 0)
			skipped := 0
			sc := bufio.NewScanner(bufio.NewReader(in))
			sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
			// Intake a window at a time through the DRUM batch path: the seen-set
			// folds each window in one sorted merge per bucket rather than shifting a
			// sorted slice per key, the O(n^2) the per-key Seed loop pays on a host's
			// run. The window is bounded so memory stays flat as the input grows.
			const seedWindow = 1 << 16
			window := make([]frontier.SeedSpec, 0, seedWindow)
			flush := func() {
				fr.SeedBatch(window)
				window = window[:0]
			}
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
				window = append(window, frontier.SeedSpec{
					URL: rec.URL, Host: host, Priority: float32(priority), CrawlDelay: delay,
				})
				if len(window) == seedWindow {
					flush()
				}
			}
			if err := sc.Err(); err != nil {
				return fmt.Errorf("read input: %w", err)
			}
			flush()

			raw, err := fr.CheckpointBytes()
			if err != nil {
				return fmt.Errorf("encode checkpoint: %w", err)
			}
			if err := os.WriteFile(out, raw, 0o644); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(),
				"seeded %d urls (%d lines skipped) into %s, %d bytes\n",
				fr.Len(), skipped, out, len(raw))
			return err
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "CDX JSONL file to read (default stdin)")
	cmd.Flags().StringVarP(&out, "out", "o", "", "path to write the .meguri checkpoint")
	cmd.Flags().Float64Var(&priority, "priority", 0.5, "initial priority for every seeded URL")
	cmd.Flags().Uint16Var(&delay, "crawl-delay", 10, "default per-host crawl delay in deciseconds")
	return cmd
}
