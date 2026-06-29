// Command meguri is the CLI front door to the frontier engine and its .meguri
// files. In M0 it carries the file tools: inspect a checkpoint, print the
// version. The crawl-loop subcommands land with the engine in later milestones.
package main

import (
	"context"
	"os"

	"github.com/charmbracelet/fang"
	"github.com/spf13/cobra"
	"github.com/tamnd/meguri"
)

func main() {
	root := &cobra.Command{
		Use:           "meguri",
		Short:         "Distributed web-crawler frontier and rescheduler",
		Long:          "meguri (巡) turns a stream of discovered links into a polite, freshness-aware crawl schedule and serializes it to .meguri files.",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       meguri.Version,
	}
	root.SetVersionTemplate("meguri {{.Version}} (" + meguri.Commit + ")\n")
	root.AddCommand(newInspectCmd())

	if err := fang.Execute(context.Background(), root); err != nil {
		os.Exit(1)
	}
}
