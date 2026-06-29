package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	meguri "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// statusOrder fixes the print order of the per-status histogram so the report is
// stable across runs (a map iteration is not). It lists every state the machine
// can land in, discovery through tombstone.
var statusOrder = []meguri.URLStatus{
	meguri.StatusDiscovered,
	meguri.StatusScheduled,
	meguri.StatusReady,
	meguri.StatusInFlight,
	meguri.StatusCrawled,
	meguri.StatusDueRecrawl,
	meguri.StatusGone,
	meguri.StatusExcludedRobots,
	meguri.StatusTrapped,
}

// newStatsCmd prints the counters of a partition or a cold file (doc 13, the
// stats command). Pointed at a directory it recovers the live partition and
// reads Frontier.Stats: the per-status URL distribution, the host and pending
// counts, the due count, and the seen-set occupancy. Pointed at a .meguri file
// it reads the footer summary without recovery, the cold form the spec names for
// a partition shipped from another machine. It is the health-and-progress view:
// the numbers that separate a frontier that is growing from one that is only
// churning.
func newStatsCmd() *cobra.Command {
	var data string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Print the counters of a partition directory or a .meguri file",
		Long:  "stats reads --data: a partition directory recovers the live frontier and prints the full per-status distribution, the pending and due counts, and the seen-set occupancy; a single .meguri file prints the footer summary (url/host counts, due range, region presence) without recovery.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if data == "" {
				return fmt.Errorf("--data is required")
			}
			info, err := os.Stat(data)
			if err != nil {
				return err
			}
			if info.IsDir() {
				return statsLive(cmd, data)
			}
			return statsCold(cmd, data)
		},
	}
	cmd.Flags().StringVar(&data, "data", "", "partition directory or .meguri file to read (required)")
	return cmd
}

// statsLive recovers the partition and prints the live Stats. The due count is
// measured against the current wall hour so it answers "how many are due now."
func statsLive(cmd *cobra.Command, dir string) error {
	p, err := engine.OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		return fmt.Errorf("open partition %s: %w", dir, err)
	}
	defer func() { _ = p.Abandon() }() // read-only: drop without a checkpoint

	now := uint32(time.Now().Unix() / 3600)
	st := p.Frontier().Stats(now)

	out := cmd.OutOrStdout()
	if _, err := fmt.Fprintf(out, "partition %s (live)\n", dir); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  urls           %d\n", st.URLs); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  hosts          %d\n", st.Hosts); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  pending        %d\n", st.Pending); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  due now        %d\n", st.Due); err != nil {
		return err
	}
	if st.NextDueHours > 0 {
		if _, err := fmt.Fprintf(out, "  next due       hour %d\n", st.NextDueHours); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(out, "  seen keys      %d\n", st.SeenKeys); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  seen bits/url  %.2f\n", st.SeenBitsPerURL); err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, "  by status\n"); err != nil {
		return err
	}
	for _, s := range statusOrder {
		if n := st.ByStatus[s]; n > 0 {
			if _, err := fmt.Fprintf(out, "    %-15s %d\n", s, n); err != nil {
				return err
			}
		}
	}
	return nil
}

// statsCold reads a .meguri file's footer summary and prints it without recovery.
// Per-status counts need the column data the live path walks, so the cold form
// reports the footer's totals and due range, the numbers the format keeps in the
// tail for exactly this read.
func statsCold(cmd *cobra.Command, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	ins, err := format.InspectBytes(raw)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", path, err)
	}
	out := cmd.OutOrStdout()
	if _, err := fmt.Fprintf(out, "partition %s (cold, footer summary)\n", path); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  partition id   %d\n", ins.PartitionID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  urls           %d\n", ins.URLCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  hosts          %d\n", ins.HostCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  host-key range 0x%016x .. 0x%016x\n", ins.HostKeyLo, ins.HostKeyHi); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  scheduled      %d\n", ins.Stats.ScheduledCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  due range      hour %d .. %d\n", ins.Stats.DueMin, ins.Stats.DueMax); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  bytes/url      %.2f\n", ins.Stats.BytesPerURL); err != nil {
		return err
	}
	hasSchedule := ins.Flags&format.FlagHasSchedule != 0
	hasSeenFilter := ins.Flags&format.FlagHasSeenset != 0
	_, err = fmt.Fprintf(out, "  schedule index %v   seen filter %v\n", hasSchedule, hasSeenFilter)
	return err
}
