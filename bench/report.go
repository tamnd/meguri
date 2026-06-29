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
