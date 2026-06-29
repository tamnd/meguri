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
