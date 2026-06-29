package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// newScheduleCmd shows what is due to be crawled (doc 13, the schedule command).
// It answers the operator's first question, "what is the crawler about to do,"
// by reading the due-time schedule rather than the whole frontier. Pointed at a
// directory it recovers the live frontier and lists the due URLs with their
// canonical strings and due hours, sorted by due time. Pointed at a .meguri file
// it reads cold through the durable schedule index: when the file carries the
// timing-wheel region it reads only the near buckets (the predicate pushdown of
// decision D13), and falls back to the next_due column scan when it does not.
func newScheduleCmd() *cobra.Command {
	var (
		data   string
		before uint32
		host   string
		limit  int
	)
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Show what is due to be crawled, by due time",
		Long:  "schedule lists the URLs whose next_due has come around. --data a directory recovers the live frontier and prints each due URL with its canonical string and due hour; --data a .meguri file reads cold through the durable schedule index (the timing wheel, when present) so it touches only the near buckets, not the whole frontier. --before sets the horizon in epoch-hours (default now), --host filters to one host key, --limit caps the list.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if data == "" {
				return fmt.Errorf("--data is required")
			}
			if before == 0 {
				before = uint32(time.Now().Unix() / 3600)
			}
			var hk uint64
			if host != "" {
				var err error
				if hk, err = parseHostKey(host); err != nil {
					return err
				}
			}
			info, err := os.Stat(data)
			if err != nil {
				return err
			}
			if info.IsDir() {
				return scheduleLive(cmd, data, before, hk, limit)
			}
			return scheduleCold(cmd, data, before, hk, limit)
		},
	}
	cmd.Flags().StringVar(&data, "data", "", "partition directory or .meguri file to read (required)")
	cmd.Flags().Uint32Var(&before, "before", 0, "due-time horizon in epoch-hours (0 = now)")
	cmd.Flags().StringVar(&host, "host", "", "filter to one host key (hex 0x... or decimal)")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum URLs to list (0 = all)")
	return cmd
}

// scheduleLive recovers the partition and lists the due URLs with their strings,
// the rich view the resident frontier can give. The partition is opened read-only
// and dropped without a checkpoint.
func scheduleLive(cmd *cobra.Command, dir string, before uint32, host uint64, limit int) error {
	p, err := engine.OpenPartition(dir, store.Options{}, frontier.WithStateMachine(), frontier.WithScheduleIndex())
	if err != nil {
		return fmt.Errorf("open partition %s: %w", dir, err)
	}
	defer func() { _ = p.Abandon() }()

	due := p.Frontier().DueURLs(before, host, limit)
	out := cmd.OutOrStdout()
	if _, err := fmt.Fprintf(out, "schedule %s (live) due at or before hour %d: %d shown\n", dir, before, len(due)); err != nil {
		return err
	}
	for _, d := range due {
		if _, err := fmt.Fprintf(out, "  hour %-8d  %s\n", d.NextDue, d.URL); err != nil {
			return err
		}
	}
	return nil
}

// scheduleCold reads a .meguri file's due keys through the schedule index. When
// the file carries the durable wheel it prunes to the near buckets (DueByWheel);
// otherwise it scans the next_due column (DueKeys). It prints the keys rather than
// the URL strings, the cold reader's pushdown form, and names which read path ran
// so the wheel's effect is visible.
func scheduleCold(cmd *cobra.Command, path string, before uint32, host uint64, limit int) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	r, err := format.NewReader(raw)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	pushdown := r.HasSchedule()
	keys, err := r.DueByWheel(before)
	if err != nil {
		return fmt.Errorf("due read: %w", err)
	}

	out := cmd.OutOrStdout()
	path0 := "next_due column scan"
	if pushdown {
		path0 = "durable schedule wheel (near buckets only)"
	}
	shown := 0
	var total int
	for _, k := range keys {
		if host != 0 && k.HostKey != host {
			continue
		}
		total++
	}
	if _, err := fmt.Fprintf(out, "schedule %s (cold, %s) due at or before hour %d: %d\n", path, path0, before, total); err != nil {
		return err
	}
	for _, k := range keys {
		if host != 0 && k.HostKey != host {
			continue
		}
		if limit > 0 && shown >= limit {
			break
		}
		if _, err := fmt.Fprintf(out, "  0x%016x:0x%016x\n", k.HostKey, k.PathKey); err != nil {
			return err
		}
		shown++
	}
	return nil
}
