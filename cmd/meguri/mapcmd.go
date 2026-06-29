package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/engine"
)

// newMapCmd prints the partition map: which partition owns which HostKey range,
// each partition's load, and the map epoch (doc 13, the map command). The live
// fleet reads the map from the control plane (--control); offline it reconstructs
// the same view from a meguri.manifest catalog (--manifest), the shared-nothing
// topology made readable without a running fleet. Given --host it resolves one
// host through the map to its owning partition, the "where does this host live"
// question a discovered link asks on every hop.
func newMapCmd() *cobra.Command {
	var (
		manifest string
		host     string
	)
	cmd := &cobra.Command{
		Use:   "map",
		Short: "Print the partition map from a fleet manifest",
		Long:  "map reads a meguri.manifest catalog (--manifest) and prints the partition map: each partition's HostKey range, URL and host counts, bytes/url, and epoch, then whether the ranges tile the key space cleanly. With --host <hexkey> it routes that single host through the map and prints its owning partition. The live control-plane source (--control) is the fleet binding; the manifest path is its offline form.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if manifest == "" {
				return fmt.Errorf("--manifest is required (the offline map source; --control is the live fleet binding)")
			}
			col, err := engine.OpenCollection(manifest)
			if err != nil {
				return fmt.Errorf("open manifest %s: %w", manifest, err)
			}
			man := col.Manifest()
			out := cmd.OutOrStdout()

			if host != "" {
				hk, err := parseHostKey(host)
				if err != nil {
					return err
				}
				if e, ok := man.Route(hk); ok {
					_, err = fmt.Fprintf(out, "host 0x%016x -> partition %d (%s)\n", hk, e.PartitionID, e.FileRef)
					return err
				}
				_, err = fmt.Fprintf(out, "host 0x%016x -> no owning partition\n", hk)
				return err
			}

			if _, err := fmt.Fprintf(out, "partition map (%d partitions)\n", len(man.Entries)); err != nil {
				return err
			}
			var urls, hosts uint64
			for _, e := range man.Entries {
				urls += e.URLCount
				hosts += e.HostCount
				if _, err := fmt.Fprintf(out,
					"  p%-5d  0x%016x .. 0x%016x  %10d urls  %7d hosts  %6.2f b/url  epoch %d\n",
					e.PartitionID, e.HostKeyLo, e.HostKeyHi, e.URLCount, e.HostCount, e.BytesPerURL, e.Epoch); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(out, "  total   %d urls  %d hosts\n", urls, hosts); err != nil {
				return err
			}
			if lo, hi, gap := man.CoverageGap(0); gap {
				_, err = fmt.Fprintf(out, "  coverage gap [0x%016x, 0x%016x]\n", lo, hi)
				return err
			}
			_, err = fmt.Fprint(out, "  ranges tile the key space cleanly\n")
			return err
		},
	}
	cmd.Flags().StringVar(&manifest, "manifest", "", "meguri.manifest catalog to read the map from (required)")
	cmd.Flags().StringVar(&host, "host", "", "resolve a single host key (hex, 0x-prefixed or decimal) through the map")
	return cmd
}

// parseHostKey reads a host key written as 0x-prefixed hex or plain decimal, the
// two forms the other commands print, so a key copied from a map or stats line
// routes straight back.
func parseHostKey(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		v, err := strconv.ParseUint(s[2:], 16, 64)
		if err != nil {
			return 0, fmt.Errorf("parse host key %q: %w", s, err)
		}
		return v, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse host key %q: %w", s, err)
	}
	return v, nil
}
