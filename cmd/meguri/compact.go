package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/format"
)

// newCompactCmd is the housekeeping command (doc 13, the compact command). It
// rewrites one or more .meguri files into a tighter form: it merges the inputs
// into one partition (the file side of consolidating partitions, D14), re-runs the
// columnar cascade so the result packs to the redistribution budget of tens of
// bytes per URL (D12), and with --gc garbage-collects the Gone tombstones a long-
// running partition accumulates, reclaiming the string arena to only the spans the
// surviving rows reference. A single input is just compacted in place into a fresh
// file; several disjoint inputs fold together, the way an LSM compaction folds runs.
func newCompactCmd() *cobra.Command {
	var (
		out string
		gc  bool
	)
	cmd := &cobra.Command{
		Use:   "compact <file...>",
		Short: "Merge .meguri files, re-run the cascade, GC tombstones",
		Long:  "compact rewrites one or more .meguri files into a tighter form. It merges the inputs into one partition (consolidation, the file side of rebalancing), re-runs the columnar cascade so the file packs to tens of bytes per URL, and with --gc drops the Gone tombstones past their re-probe horizon and reclaims the string arena. --out is where to write the result (default compact.meguri next to the first input). Inputs must own disjoint, ordered HostKey ranges; an overlap is reported rather than producing a file a reader would reject.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				out = filepath.Join(filepath.Dir(args[0]), "compact.meguri")
			}

			merged, err := mergeFiles(args)
			if err != nil {
				return err
			}
			urlsBefore := len(merged.URLs)
			if gc {
				merged = format.Compact(merged)
			}
			blob, err := format.Encode(merged)
			if err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			if err := os.WriteFile(out, blob, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}

			out0 := cmd.OutOrStdout()
			dropped := urlsBefore - len(merged.URLs)
			if gc {
				_, err = fmt.Fprintf(out0, "compacted %d file(s) -> %s: %d urls (%d Gone dropped), %d bytes\n", len(args), out, len(merged.URLs), dropped, len(blob))
			} else {
				_, err = fmt.Fprintf(out0, "compacted %d file(s) -> %s: %d urls, %d bytes (use --gc to drop Gone tombstones)\n", len(args), out, len(merged.URLs), len(blob))
			}
			return err
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "path to write the compacted .meguri file (default compact.meguri by the first input)")
	cmd.Flags().BoolVar(&gc, "gc", false, "garbage-collect Gone tombstones and reclaim the string arena")
	return cmd
}

// mergeFiles decodes each input file and folds them into one partition. A single
// input decodes straight through; several disjoint inputs merge left-to-right,
// surfacing format.ErrNotSorted as a readable overlap error rather than writing a
// file a reader would reject.
func mergeFiles(paths []string) (*format.Partition, error) {
	var acc *format.Partition
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		part, err := format.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", p, err)
		}
		if acc == nil {
			acc = part
			continue
		}
		acc, err = format.Merge(acc, part)
		if err != nil {
			return nil, fmt.Errorf("merge %s: %w (inputs must own disjoint, ordered HostKey ranges)", p, err)
		}
	}
	return acc, nil
}
