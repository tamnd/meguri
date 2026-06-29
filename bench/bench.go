// Package bench assembles the deterministic per-partition costs of doc 14 into
// the hundred-billion projection. It measures the two numbers that are counted,
// not timed, on a real corpus partition: the .meguri bytes per URL (section 3.8)
// and the seen-set bits per URL with its achieved false-positive rate (section
// 3.7). Then it multiplies each by a stated partition count to project the fleet
// totals (section 6), keeping the multiplication visible so a reader can
// substitute their own count and recompute.
//
// The split is the one doc 14 section 2 draws: byte and bit counts are
// deterministic, so they need no best-of-N and no contention defense and live
// here; the contended latencies (schedule selection, queue ops, dedup
// throughput) stay in the go test -bench micro-benchmarks where benchstat can
// defend them. This package owns only the part of the proof that is a count.
package bench

import (
	"fmt"
	"math"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/format"
)

// errEmpty is returned when there is nothing to measure, so a caller does not
// divide a byte count by a zero URL count.
var errEmpty = fmt.Errorf("bench: partition has no urls to measure")

// Measured holds the deterministic per-partition costs measured on a real
// partition. Every field is a counted quantity, not a target and not a timing,
// so it carries no best-of-N: the file is encoded once and its bytes counted,
// the seen-set is filled once and its bits counted. These are the numbers the
// fleet projection multiplies, so they are reported as measured before any
// projection is allowed to use them (doc 14, sections 3.7, 3.8, the D19 rule).
type Measured struct {
	URLs      int // distinct urls in the partition, the divisor for every per-url cost
	Hosts     int // distinct hosts
	FileBytes int // the encoded .meguri size

	BytesPerURL float64      // FileBytes / URLs, the section 3.8 redistribution number
	Regions     []RegionCost // per-region byte breakdown from the footer directory

	SeenSetBits int     // resident filter bits over the held keys
	BitsPerURL  float64 // SeenSetBits / URLs, the section 3.7 seen-set number

	FPProbes       int     // held-out keys probed for a filter false positive
	FPHits         int     // probes the filter false-positived on
	FPRate         float64 // FPHits / FPProbes, the rate achieved at BitsPerURL
	FalseNegatives int     // held keys the one-sided filter missed; must be 0
}

// RegionCost is one row of the .meguri byte breakdown: a named region, its bytes,
// and its share per URL. The breakdown is not optional (doc 14, section 3.8): it
// shows where the compression came from and which regions are the floor.
type RegionCost struct {
	Name        string
	Bytes       uint64
	BytesPerURL float64
}

// Measure counts the deterministic per-partition costs on one real partition. It
// encodes the partition to a .meguri file and counts the bytes per URL and per
// region, then fills a seen-set sized to the real key count and counts the bits
// per URL, the achieved false-positive rate against held-out probe keys, and the
// false-negative count that a one-sided filter must keep at zero. It mutates
// nothing the caller owns and times nothing, so the result is reproducible to
// the byte.
func Measure(part *format.Partition) (Measured, error) {
	n := len(part.URLs)
	if n == 0 {
		return Measured{}, errEmpty
	}

	raw, err := format.Encode(part)
	if err != nil {
		return Measured{}, fmt.Errorf("bench: encode partition: %w", err)
	}
	ins, err := format.InspectBytes(raw)
	if err != nil {
		return Measured{}, fmt.Errorf("bench: inspect encoded file: %w", err)
	}

	out := Measured{
		URLs:        n,
		Hosts:       len(part.Hosts),
		FileBytes:   len(raw),
		BytesPerURL: float64(len(raw)) / float64(n),
	}
	for _, r := range ins.Regions {
		out.Regions = append(out.Regions, RegionCost{
			Name:        r.Name,
			Bytes:       r.Length,
			BytesPerURL: float64(r.Length) / float64(n),
		})
	}

	// Fill a seen-set sized to the real key count so the bits-per-URL is measured
	// at the real fill, not at a default capacity that would understate it.
	s := dedup.NewSeenSet(dedup.WithCapacity(uint64(n)))
	for _, u := range part.URLs {
		s.Insert(u.URLKey)
	}
	out.BitsPerURL = s.BitsPerURL()
	out.SeenSetBits = int(math.Round(out.BitsPerURL * float64(n)))

	// The one-sided filter must report every key it has seen. A miss here is a
	// false negative, which would mean a dropped page, so the count must be 0.
	for _, u := range part.URLs {
		if !s.MaybeContains(u.URLKey) {
			out.FalseNegatives++
		}
	}

	// Achieved false-positive rate: probe keys the set never held, one synthetic
	// path per real host so the host distribution stays real, and count the
	// fraction the filter calls "probably seen". A probe that happens to collide
	// with a real key is skipped so the rate counts only genuine absences.
	probed, hits := 0, 0
	for i, u := range part.URLs {
		pk := m.URLKey{HostKey: u.HostKey, PathKey: probePath(i)}
		if s.Contains(pk) {
			continue // a real collision, not an absence; do not count it
		}
		probed++
		if s.MaybeContains(pk) {
			hits++
		}
	}
	out.FPProbes = probed
	out.FPHits = hits
	if probed > 0 {
		out.FPRate = float64(hits) / float64(probed)
	}
	return out, nil
}

// probePath derives a PathKey for a key the corpus does not contain, from a path
// string no canonical URL would produce. The host half stays a real HostKey, so
// the probe key sits in the real key space but at an absent path.
func probePath(i int) uint64 {
	return m.PathKeyOf(fmt.Sprintf("/__meguri_bench_probe_%d", i))
}

// Projection is the section 6 fleet projection: each fleet total is one measured
// per-partition cost times a stated count. The multiplication is kept as a
// string next to the product so a reader can read off the per-partition number
// and the count, substitute their own count, and recompute, which is the form
// D19 requires of every hundred-billion figure.
type Projection struct {
	TotalURLs        float64 // the canon scale target, 100 billion
	URLsPerPartition float64 // the pinned per-partition capacity, the lever
	PartitionCount   float64 // TotalURLs / URLsPerPartition, derived not assumed

	SeenSetFleetBytes float64 // measured bits/url x TotalURLs / 8
	SeenSetFleetCalc  string  // the multiplication, shown
	SeenSetPerPart    float64 // SeenSetFleetBytes / PartitionCount, the per-machine share

	MeguriFleetBytes float64 // measured bytes/url x TotalURLs
	MeguriFleetCalc  string  // the multiplication, shown
	MeguriPerPart    float64 // MeguriFleetBytes / PartitionCount, one file per partition
}

// Project multiplies the measured per-partition costs out to the fleet totals at
// a stated total URL count and per-partition capacity. The partition count falls
// out as total/per-partition rather than being assumed, so the count is derived
// from a real measured capacity (doc 14, section 6.1). Both fleet totals are the
// measured-times-count form, with the multiplication captured in the *Calc
// strings.
func Project(meas Measured, totalURLs, urlsPerPartition float64) Projection {
	pc := math.NaN()
	if urlsPerPartition > 0 {
		pc = totalURLs / urlsPerPartition
	}

	seenFleet := meas.BitsPerURL * totalURLs / 8
	fileFleet := meas.BytesPerURL * totalURLs

	p := Projection{
		TotalURLs:         totalURLs,
		URLsPerPartition:  urlsPerPartition,
		PartitionCount:    pc,
		SeenSetFleetBytes: seenFleet,
		SeenSetFleetCalc: fmt.Sprintf("%.2f bits/url x %s / 8 = %s",
			meas.BitsPerURL, sci(totalURLs), humanBytes(seenFleet)),
		MeguriFleetBytes: fileFleet,
		MeguriFleetCalc: fmt.Sprintf("%.2f bytes/url x %s = %s",
			meas.BytesPerURL, sci(totalURLs), humanBytes(fileFleet)),
	}
	if pc > 0 {
		p.SeenSetPerPart = seenFleet / pc
		p.MeguriPerPart = fileFleet / pc
	}
	return p
}

// Wall names a physical floor a meguri number is reported against, never around
// (doc 14, section 10). The politeness floor this package measures from the
// partition's own crawl-delay distribution; the fsync and device-bandwidth
// floors are device properties named here with their formula and measured by the
// hardware gates, because a byte-counting pass cannot measure a disk.
type Wall struct {
	Name    string
	Formula string
	Note    string
}

// Walls returns the three honest walls, with the politeness floor filled in from
// the partition's host crawl delays. The single-host floor is 1/crawl_delay by
// the definition of politeness, and the partition's polite dispatch rate is the
// sum over its hosts of one-over-each-delay, the host-parallelism sum doc 14
// section 5.3 reports against. The fsync and bandwidth floors carry their
// formula and the box that measures them, not a number this pass invented.
func Walls(part *format.Partition) []Wall {
	single, summed, hosts := politeness(part)
	politenessNote := "no host crawl delays in this partition"
	if hosts > 0 {
		politenessNote = fmt.Sprintf(
			"%d hosts, median single-host floor %.3f fetch/s, summed polite dispatch %.1f fetch/s over this partition's hosts",
			hosts, single, summed)
	}
	return []Wall{
		{
			Name:    "politeness floor",
			Formula: "single host <= 1/crawl_delay; partition dispatch = sum over hosts of 1/crawl_delay",
			Note:    politenessNote,
		},
		{
			Name:    "fsync floor (concurrency 1)",
			Formula: "durable commit rate <= 1/fsync_latency; group-commit amortizes toward fsync/batch",
			Note:    "device floor, measured on the GamingPC NVMe by the live-store gate, not by this pass",
		},
		{
			Name:    "device bandwidth on redistribution",
			Formula: "partition move time >= file_bytes / min(disk_read_bw, net_write_bw)",
			Note:    "device floor, measured on the GamingPC by the distribution gate; file_bytes is the section 3.8 measurement",
		},
	}
}

// politeness reduces the partition's host crawl delays to the single-host floor
// at the median delay and the summed polite dispatch rate over all hosts. The
// crawl delay is stored in deciseconds, so a delay of d deciseconds is d/10
// seconds and yields 10/d fetches per second from that host.
func politeness(part *format.Partition) (singleMedian, summed float64, hosts int) {
	delays := make([]uint16, 0, len(part.Hosts))
	for _, h := range part.Hosts {
		d := h.CrawlDelay
		if d == 0 {
			continue
		}
		delays = append(delays, d)
		summed += 10.0 / float64(d)
	}
	hosts = len(delays)
	if hosts == 0 {
		return 0, 0, 0
	}
	// Median delay by selection over the small per-partition host count.
	insertionSort(delays)
	med := delays[hosts/2]
	return 10.0 / float64(med), summed, hosts
}

func insertionSort(a []uint16) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// naiveKeyBits is the exact-membership floor of a naive key-value frontier: one
// 128-bit URLKey stored per seen URL, the smallest an exact "have I seen this"
// set can be. A real naive frontier pays more on top (a RocksDB-backed set adds
// the SST block index, a per-SST bloom filter, and write amplification; a Nutch
// CrawlDb keeps the whole CrawlDatum per URL; Frontera's store keeps the URL
// string), so this is the optimistic lower bound the externals sit above, not a
// number that flatters meguri.
const naiveKeyBits = 128

// Baseline is the doc 14 section 7 paired comparison of meguri's seen-set against
// a naive exact-key frontier. It is the deterministic, counts-not-timed arm of the
// baseline comparison: the memory a naive frontier must spend to answer the same
// dedup question, paired against meguri's measured cost, projected to the fleet.
// The named external systems (Nutch, a RocksDB frontier, Frontera) are cited
// separately because measuring their real overhead needs the systems themselves
// on a fleet box; this arm fixes the floor they cannot beat.
type Baseline struct {
	NaiveBitsPerURL  float64 // 128, the exact-key floor of a naive frontier
	MeguriBitsPerURL float64 // measured seen-set bits/url
	MemoryRatio      float64 // naive / meguri, the paired ratio
	NaiveFleetBytes  float64 // naive bits/url x TotalURLs / 8
	MeguriFleetBytes float64 // meguri bits/url x TotalURLs / 8
	Calc             string  // the paired multiplication, shown
}

// NaiveFrontierBaseline builds the seen-set baseline comparison: the naive
// exact-key store at 128 bits/url against meguri's measured seen-set bits/url,
// both projected to the stated fleet total. The ratio is the paired number doc 14
// section 7 asks for, the factor by which the approximate tiered filter undercuts
// an exact key set before any LSM or per-record overhead is added.
func NaiveFrontierBaseline(meas Measured, totalURLs float64) Baseline {
	naiveFleet := naiveKeyBits * totalURLs / 8
	meguriFleet := meas.BitsPerURL * totalURLs / 8
	ratio := math.NaN()
	if meas.BitsPerURL > 0 {
		ratio = naiveKeyBits / meas.BitsPerURL
	}
	return Baseline{
		NaiveBitsPerURL:  naiveKeyBits,
		MeguriBitsPerURL: meas.BitsPerURL,
		MemoryRatio:      ratio,
		NaiveFleetBytes:  naiveFleet,
		MeguriFleetBytes: meguriFleet,
		Calc: fmt.Sprintf("naive %d bits/url -> %s vs meguri %.2f bits/url -> %s (%.1fx)",
			naiveKeyBits, humanBytes(naiveFleet), meas.BitsPerURL, humanBytes(meguriFleet), ratio),
	}
}

// PolitenessPoint is one point on the polite-dispatch ceiling curve: when the
// ActiveHosts fastest hosts of a partition are simultaneously dispatchable, the
// partition can be fetched at most CeilingFPS fetches per second without breaking
// any host's crawl delay.
type PolitenessPoint struct {
	ActiveHosts int
	CeilingFPS  float64
}

// PolitenessCurve returns the polite-dispatch ceiling at each active-host count
// in ks, the curve doc 14 section 5.3 plots throughput against. It sorts the
// partition's hosts by their per-host fetch rate (fastest first) and prefix-sums,
// so the ceiling at k active hosts is the most a crawler could politely fetch if
// it kept the k fastest hosts busy. The shape is the central scaling fact of a
// polite crawler: throughput rises with the number of active hosts and with
// nothing else, which is why a frontier that holds millions of hosts resident is
// the lever, not a faster scheduler (IRLbot section 4, BUbiNG section 3). A k
// past the host count is clamped to the whole partition.
func PolitenessCurve(part *format.Partition, ks []int) []PolitenessPoint {
	rates := make([]float64, 0, len(part.Hosts))
	for _, h := range part.Hosts {
		if h.CrawlDelay == 0 {
			continue
		}
		rates = append(rates, 10.0/float64(h.CrawlDelay)) // deciseconds -> fetches/s
	}
	// Fastest hosts first, so the prefix sum is the best a crawler could do with k
	// active hosts. Insertion sort over the small per-partition host count.
	for i := 1; i < len(rates); i++ {
		for j := i; j > 0 && rates[j] > rates[j-1]; j-- {
			rates[j], rates[j-1] = rates[j-1], rates[j]
		}
	}
	out := make([]PolitenessPoint, 0, len(ks))
	for _, k := range ks {
		if k > len(rates) {
			k = len(rates)
		}
		if k <= 0 {
			out = append(out, PolitenessPoint{ActiveHosts: 0})
			continue
		}
		var sum float64
		for i := range k {
			sum += rates[i]
		}
		out = append(out, PolitenessPoint{ActiveHosts: k, CeilingFPS: sum})
	}
	return out
}

// Throughput is the doc 14 section 5.3 throughput analysis: the scheduler's own
// selection rate measured against the politeness ceiling the same partition
// imposes, so the gap between them is named, never hidden. The scheduler rate is
// the measured selections-per-second the operator passes in from
// BenchmarkCorpusDispatchSelections (this byte-and-curve pass does not time a
// loop); everything else is computed from the partition's real crawl delays.
type Throughput struct {
	SchedulerFPS     float64           // measured scheduler selections/s, the operator's benchstat figure
	PoliteCeilingFPS float64           // sum over all hosts of 1/crawl_delay, the partition's polite ceiling
	MedianHostFPS    float64           // 1/crawl_delay at the median host, the per-host floor
	ActiveHosts      int               // hosts with a crawl delay, the ceiling's host count
	FetcherBoundGap  float64           // SchedulerFPS / PoliteCeilingFPS, how many times the scheduler outruns this host set
	HostsToSaturate  float64           // active hosts (at the median delay) needed before the scheduler stops being the slack
	Curve            []PolitenessPoint // the ceiling at a few active-host counts
}

// Analyze builds the throughput analysis for a partition given the measured
// scheduler selection rate. The fetcher-bound gap is the scheduler rate over the
// polite ceiling: a gap far above one means the crawler is fetcher-bound, that
// the scheduler finishes its selection long before politeness lets the next fetch
// go, the regime every polite web crawler sits in. The hosts-to-saturate count is
// the scheduler rate divided by the median single-host rate, the number of active
// hosts a partition would need before the scheduler itself became the bottleneck.
func Analyze(part *format.Partition, schedulerFPS float64) Throughput {
	median, summed, hosts := politeness(part)
	t := Throughput{
		SchedulerFPS:     schedulerFPS,
		PoliteCeilingFPS: summed,
		MedianHostFPS:    median,
		ActiveHosts:      hosts,
		Curve:            PolitenessCurve(part, curveLadder(hosts)),
	}
	if summed > 0 {
		t.FetcherBoundGap = schedulerFPS / summed
	}
	if median > 0 {
		t.HostsToSaturate = schedulerFPS / median
	}
	return t
}

// curveLadder builds the active-host counts the politeness curve is sampled at:
// the powers of ten strictly below the partition's host count, then the host
// count itself, so the curve always ends at the whole partition and never repeats
// a clamped point. A single-host partition yields just {1}.
func curveLadder(hosts int) []int {
	if hosts <= 1 {
		return []int{max(hosts, 0)}
	}
	var ks []int
	for k := 1; k < hosts; k *= 10 {
		ks = append(ks, k)
	}
	return append(ks, hosts)
}

// sci renders a large count in the 1eN form the projection states its
// assumptions in, so 100 billion reads as 1.00e11 and lines up with the spec.
func sci(v float64) string {
	if v == 0 {
		return "0"
	}
	exp := math.Floor(math.Log10(math.Abs(v)))
	mant := v / math.Pow(10, exp)
	return fmt.Sprintf("%.2fe%d", mant, int(exp))
}

// humanBytes renders a byte count in the GB/TB units the fleet totals are
// stated in, so the projected seen-set reads as ~125 GB and the .meguri fleet as
// ~3 TB without losing the underlying number.
func humanBytes(b float64) string {
	const (
		kb = 1e3
		mb = 1e6
		gb = 1e9
		tb = 1e12
	)
	switch {
	case b >= tb:
		return fmt.Sprintf("%.2f TB", b/tb)
	case b >= gb:
		return fmt.Sprintf("%.2f GB", b/gb)
	case b >= mb:
		return fmt.Sprintf("%.2f MB", b/mb)
	case b >= kb:
		return fmt.Sprintf("%.2f KB", b/kb)
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}
