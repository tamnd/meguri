// Command meguri is the CLI front door to the frontier engine and its .meguri
// files. It carries the file tools: seed a frontier from a URL list, inspect a
// checkpoint, print the version. The crawl-loop subcommands that drive ami land
// with the distributed engine in later milestones.
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
	root.AddCommand(newSeedCmd())

	if err := fang.Execute(context.Background(), root); err != nil {
		os.Exit(1)
	}
}
