package bench

import (
	"fmt"
	"strings"
)

// Report renders the measured per-partition costs, the fleet projection, and the
// three named walls as one stable human-readable block, the form `meguri bench`
// prints. It is the assembled D19 statement: every fleet number sits next to the
// measured per-partition cost and the count it was multiplied by, and every wall
// is named with its formula, so nothing fleet-scale appears as a bare round
// number.
func Report(meas Measured, proj Projection, walls []Wall) string {
	var b strings.Builder

	fmt.Fprintf(&b, "measured per-partition (real corpus, %d urls / %d hosts)\n", meas.URLs, meas.Hosts)
	fmt.Fprintf(&b, "  .meguri file        %d bytes\n", meas.FileBytes)
	fmt.Fprintf(&b, "  bytes/url           %.2f\n", meas.BytesPerURL)
	for _, r := range meas.Regions {
		fmt.Fprintf(&b, "    %-12s      %.2f bytes/url (%d bytes)\n", r.Name, r.BytesPerURL, r.Bytes)
	}
	fmt.Fprintf(&b, "  seen-set bits/url   %.2f\n", meas.BitsPerURL)
	fmt.Fprintf(&b, "  achieved fp rate    %.4f (%d/%d probes)\n", meas.FPRate, meas.FPHits, meas.FPProbes)
	fmt.Fprintf(&b, "  false negatives     %d\n", meas.FalseNegatives)

	b.WriteString("\nfleet projection (measured per-partition x stated count)\n")
	fmt.Fprintf(&b, "  total frontier      %s urls\n", sci(proj.TotalURLs))
	fmt.Fprintf(&b, "  urls/partition      %s\n", sci(proj.URLsPerPartition))
	fmt.Fprintf(&b, "  partition count     %.0f (= total / urls-per-partition)\n", proj.PartitionCount)
	fmt.Fprintf(&b, "  seen-set fleet      %s\n", proj.SeenSetFleetCalc)
	fmt.Fprintf(&b, "                      %s per partition\n", humanBytes(proj.SeenSetPerPart))
	fmt.Fprintf(&b, "  .meguri fleet       %s\n", proj.MeguriFleetCalc)
	fmt.Fprintf(&b, "                      %s per partition\n", humanBytes(proj.MeguriPerPart))

	b.WriteString("\nthe walls (reported against, never around)\n")
	for _, w := range walls {
		fmt.Fprintf(&b, "  %s\n", w.Name)
		fmt.Fprintf(&b, "    %s\n", w.Formula)
		fmt.Fprintf(&b, "    %s\n", w.Note)
	}
	return b.String()
}

// ThroughputReport renders the doc 14 section 5.3 throughput analysis: the
// measured scheduler selection rate against the politeness ceiling the same
// partition imposes, the fetcher-bound gap between them, and the polite-dispatch
// ceiling as the number of active hosts grows. It is printed under the main
// report so the headline scheduler number never stands without the host-set
// ceiling it actually runs against.
func ThroughputReport(t Throughput) string {
	var b strings.Builder
	b.WriteString("throughput analysis (scheduler vs politeness, doc 14 section 5.3)\n")
	fmt.Fprintf(&b, "  scheduler selection   %s sel/s (measured, BenchmarkCorpusDispatchSelections)\n", sci(t.SchedulerFPS))
	fmt.Fprintf(&b, "  polite ceiling        %.1f fetch/s over %d active hosts (sum of 1/crawl_delay)\n", t.PoliteCeilingFPS, t.ActiveHosts)
	fmt.Fprintf(&b, "  median host floor     %.3f fetch/s (1/crawl_delay at the median host)\n", t.MedianHostFPS)
	fmt.Fprintf(&b, "  fetcher-bound gap     %sx (scheduler outruns this host set by this factor)\n", sci(t.FetcherBoundGap))
	fmt.Fprintf(&b, "  active hosts to saturate the scheduler   %s (= scheduler rate / median host floor)\n", sci(t.HostsToSaturate))
	b.WriteString("  polite ceiling vs active hosts\n")
	for _, p := range t.Curve {
		fmt.Fprintf(&b, "    %8d hosts   %12.1f fetch/s\n", p.ActiveHosts, p.CeilingFPS)
	}
	return b.String()
}

// RebalanceReport renders the doc 12 section 8 rebalance-vs-bandwidth arm: when
// the fleet grows from one source partition to NewParts, how many hosts and URLs
// move, the .meguri bytes that ships, and the transfer-time floor at the stated
// device bandwidth. It is printed under the walls so the redistribution floor that
// Walls names in formula gets a measured count beside it. The bandwidth is the
// caller-named wall, not a measured disk; the fleet-box re-run that times a real
// link is the companion noted in the block.
func RebalanceReport(c RebalanceCost) string {
	var b strings.Builder
	fmt.Fprintf(&b, "rebalance vs bandwidth (1 -> %d partitions, doc 12 section 8)\n", c.NewParts)
	fmt.Fprintf(&b, "  source partition      %d hosts / %d urls / %s\n", c.SourceHosts, c.SourceURLs, humanBytes(float64(c.SourceBytes)))
	fmt.Fprintf(&b, "  moved by jump hash    %d hosts / %d urls to %d destinations\n", c.MovedHosts, c.MovedURLs, c.Destinations)
	fmt.Fprintf(&b, "  shipped               %s (%.1f%% of the source)\n", humanBytes(float64(c.ShippedBytes)), c.MovedFraction*100)
	fmt.Fprintf(&b, "  transfer floor        %.4f s at %.0f MB/s (shipped bytes / bandwidth)\n", c.TransferSec, c.BandwidthMBps)
	b.WriteString("  the bandwidth is the named device wall, not a measured disk; the at-scale re-run that times a\n")
	b.WriteString("  real NVMe and link on the fleet box is the timed companion.\n")
	return b.String()
}

// BaselineReport renders the doc 14 section 7 seen-set baseline comparison: the
// naive exact-key frontier floor paired against meguri's measured seen-set, with
// the named external systems cited as the floor's ceiling, not measured here.
func BaselineReport(bl Baseline) string {
	var b strings.Builder
	b.WriteString("baseline comparison (seen-set vs a naive exact-key frontier, doc 14 section 7)\n")
	fmt.Fprintf(&b, "  naive exact-key store   %.0f bits/url (one 128-bit URLKey per seen URL)\n", bl.NaiveBitsPerURL)
	fmt.Fprintf(&b, "  meguri seen-set         %.2f bits/url (measured, tiered filter)\n", bl.MeguriBitsPerURL)
	fmt.Fprintf(&b, "  memory ratio            %.1fx smaller\n", bl.MemoryRatio)
	fmt.Fprintf(&b, "  fleet                   %s\n", bl.Calc)
	b.WriteString("  externals cited separately: a RocksDB-backed set adds SST index, per-SST bloom, and write\n")
	b.WriteString("  amplification; Nutch keeps the whole CrawlDatum per URL; Frontera stores the URL string. The\n")
	b.WriteString("  128-bit floor is the optimistic lower bound they sit above; their measured overhead is the\n")
	b.WriteString("  fleet-box follow-up.\n")
	return b.String()
}
