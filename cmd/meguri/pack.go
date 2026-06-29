package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// newPackCmd is the explicit checkpoint command (doc 13, the pack command). It
// writes a partition's current live state to a fresh self-describing .meguri file:
// the URL table, host table, schedule index, seen-set filter, and string and blob
// region serialized into one columnar file with the footer written last (D12), the
// partition's snapshot, redistribution unit, and cold archive all at once (D1). It
// is how an operator forces a durable snapshot, ships a partition to another
// machine, or archives a frontier slice. The directory is opened read-only and
// dropped without a checkpoint, so packing never mutates the live partition; the
// output is what a fresh checkpoint of that partition would write.
func newPackCmd() *cobra.Command {
	var (
		data string
		out  string
	)
	cmd := &cobra.Command{
		Use:   "pack",
		Short: "Write a partition's live state to a fresh .meguri file",
		Long:  "pack serializes a partition's current live state to a fresh, self-describing .meguri file (the explicit checkpoint command). --data is the partition directory to read; --out is where to write the file (default <data>/pack.meguri). The directory is opened read-only and dropped without a checkpoint, so packing never mutates the live partition. The output is a file any meguri inspect can open anywhere.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if data == "" {
				return fmt.Errorf("--data is required")
			}
			info, err := os.Stat(data)
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return fmt.Errorf("--data %s is not a partition directory", data)
			}
			if out == "" {
				out = filepath.Join(data, "pack.meguri")
			}

			p, err := engine.OpenPartition(data, store.Options{}, frontier.WithStateMachine(), frontier.WithScheduleIndex())
			if err != nil {
				return fmt.Errorf("open partition %s: %w", data, err)
			}
			defer func() { _ = p.Abandon() }()

			blob, err := p.Frontier().CheckpointBytes()
			if err != nil {
				return fmt.Errorf("checkpoint: %w", err)
			}
			if err := os.WriteFile(out, blob, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "packed %s -> %s (%d bytes)\n", data, out, len(blob))
			return err
		},
	}
	cmd.Flags().StringVar(&data, "data", "", "partition directory to read (required)")
	cmd.Flags().StringVar(&out, "out", "", "path to write the .meguri file (default <data>/pack.meguri)")
	return cmd
}
