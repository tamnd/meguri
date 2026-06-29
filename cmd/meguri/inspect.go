package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/format"
)

// newInspectCmd reads a .meguri file's header and footer and prints the
// structural summary. It reads the whole file for simplicity in M0; the summary
// itself is computed from the tail, so the cost is the header plus footer, not
// the column data.
func newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <file.meguri>",
		Short: "Print the structure and stats of a .meguri file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			raw, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			ins, err := format.InspectBytes(raw)
			if err != nil {
				return fmt.Errorf("inspect %s: %w", args[0], err)
			}
			_, err = fmt.Fprint(cmd.OutOrStdout(), ins.String())
			return err
		},
	}
}
