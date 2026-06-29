package bench

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
)

// corpusPath returns the corpus path or "" when none is configured, so every
// gate skips cleanly on a machine that has not pulled the slice.
func corpusPath() string { return os.Getenv("MEGURI_CORPUS") }

// loadCorpusPartition builds a real partition from the frozen ccrawl slice by
// seeding a frontier exactly as the seed path does and round-tripping it through
// the on-disk format. The partition carries its URL strings, so the measured
// bytes-per-URL is the honest .meguri cost, not an in-memory shortcut.
func loadCorpusPartition(tb testing.TB, path string) *format.Partition {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type rec struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	}
	fr := frontier.New(0, 0)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r rec
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		host := r.Host
		if host == "" {
			host = frontier.HostOf(r.URL)
		}
		if host == "" {
			continue
		}
		fr.Seed(r.URL, host, 0.5, 0, 0, 10)
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	raw, err := fr.CheckpointBytes()
	if err != nil {
		tb.Fatalf("checkpoint: %v", err)
	}
	part, err := format.Decode(raw)
	if err != nil {
		tb.Fatalf("decode checkpoint: %v", err)
	}
	return part
}

// TestProjectMultiplication checks the section-6 form on the canon's own worked
// numbers: at 10 bits/url a hundred billion urls is ~125 GB of seen-set, at 7
// bits/url (ribbon) ~88 GB, and at 30 bytes/url the .meguri fleet is ~3 TB. The
// partition count is total/per-partition, derived not assumed. This is the
// multiplication doc 14 section 6.2 writes out, checked to the byte.
func TestProjectMultiplication(t *testing.T) {
	const total = 100e9

	bloom := Project(Measured{BitsPerURL: 10, BytesPerURL: 30}, total, 30e6)
	if got := bloom.SeenSetFleetBytes; math.Abs(got-1.25e11) > 1 {
		t.Fatalf("seen-set fleet at 10 bits/url = %.0f, want 1.25e11", got)
	}
	if got := bloom.MeguriFleetBytes; math.Abs(got-3e12) > 1 {
		t.Fatalf(".meguri fleet at 30 bytes/url = %.0f, want 3e12", got)
	}
	if got := bloom.PartitionCount; math.Abs(got-3333.33) > 0.1 {
		t.Fatalf("partition count at 30M/partition = %.2f, want ~3333.33", got)
	}

	ribbon := Project(Measured{BitsPerURL: 7}, total, 100e6)
	if got := ribbon.SeenSetFleetBytes; math.Abs(got-8.75e10) > 1 {
		t.Fatalf("ribbon seen-set fleet at 7 bits/url = %.0f, want 8.75e10", got)
	}
	if got := ribbon.PartitionCount; math.Abs(got-1000) > 0.1 {
		t.Fatalf("partition count at 100M/partition = %.2f, want 1000", got)
	}

	// The fleet total divided across the partition count is the per-machine share,
	// so per-partition x count round-trips back to the fleet total.
	if got := bloom.SeenSetPerPart * bloom.PartitionCount; math.Abs(got-bloom.SeenSetFleetBytes) > 1 {
		t.Fatalf("seen-set per-partition x count = %.0f, want the fleet total %.0f", got, bloom.SeenSetFleetBytes)
	}
}

// TestMeasureSmallPartition checks Measure on a hand-built partition: bytes/url
// is positive, the seen-set holds every key with no false negative, and the
// achieved fp rate is a fraction. It is the cheap unit gate that runs with no
// corpus, so the logic is covered even where the slice is absent.
func TestMeasureSmallPartition(t *testing.T) {
	var urls []m.URLRecord
	hostKey := m.HostKeyOf("a.example")
	for i := range 500 {
		k := m.URLKey{HostKey: hostKey, PathKey: m.PathKeyOf("/p/" + itoa(i))}
		urls = append(urls, m.URLRecord{URLKey: k, HostKey: hostKey, Status: m.StatusScheduled, HTTPStatus: 200})
	}
	sortURLs(urls)
	hosts := []m.HostRecord{{HostKey: hostKey, Grouping: m.GroupFullHost, CrawlDelay: 10}}
	part := format.Pack(0, hostKey, hostKey, 1000, format.CodecZstd, urls, hosts, nil)

	meas, err := Measure(part)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if meas.URLs != 500 {
		t.Fatalf("urls = %d, want 500", meas.URLs)
	}
	if meas.BytesPerURL <= 0 {
		t.Fatalf("bytes/url = %.2f, want positive", meas.BytesPerURL)
	}
	if meas.FalseNegatives != 0 {
		t.Fatalf("false negatives = %d, want 0", meas.FalseNegatives)
	}
	if meas.BitsPerURL <= 0 {
		t.Fatalf("bits/url = %.2f, want positive", meas.BitsPerURL)
	}
	if meas.FPRate < 0 || meas.FPRate > 1 {
		t.Fatalf("fp rate = %.4f, want a fraction", meas.FPRate)
	}
}

// TestMeasureEmptyPartitionErrors makes sure Measure refuses an empty partition
// rather than dividing a byte count by zero urls.
func TestMeasureEmptyPartitionErrors(t *testing.T) {
	if _, err := Measure(&format.Partition{}); err == nil {
		t.Fatal("measure of an empty partition should error")
	}
}

// TestBenchOnCorpus is the M10 gate on real data: build a partition from the
// frozen CC-MAIN-2026-25 slice, measure the deterministic per-partition costs,
// and require them to land in the canon's targets before the projection uses
// them. Bytes/url must be well under the ~175-190 raw-row floor (doc 03), into
// the tens-of-bytes target; bits/url must be near 10 at a fp rate near 1%; and
// the filter must make zero false negatives. The projection multiplications must
// reproduce the section-6 fleet totals from the measured numbers.
func TestBenchOnCorpus(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	part := loadCorpusPartition(t, path)
	if len(part.URLs) < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000 for a meaningful measurement", len(part.URLs))
	}

	meas, err := Measure(part)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}

	if meas.FalseNegatives != 0 {
		t.Fatalf("the one-sided filter made %d false negatives, must be 0", meas.FalseNegatives)
	}
	if meas.BytesPerURL <= 0 || meas.BytesPerURL >= 175 {
		t.Fatalf("bytes/url = %.2f, want under the ~175 raw-row floor (target tens of bytes)", meas.BytesPerURL)
	}
	if meas.BitsPerURL < 8 || meas.BitsPerURL > 16 {
		t.Fatalf("seen-set bits/url = %.2f, want near the 10-bit budget", meas.BitsPerURL)
	}
	if meas.FPRate > 0.03 {
		t.Fatalf("achieved fp rate = %.4f, want near the 1%% budget", meas.FPRate)
	}

	proj := Project(meas, 100e9, 30e6)
	wantSeen := meas.BitsPerURL * 100e9 / 8
	if math.Abs(proj.SeenSetFleetBytes-wantSeen) > 1 {
		t.Fatalf("seen-set fleet projection %.0f does not match bits/url x count %.0f", proj.SeenSetFleetBytes, wantSeen)
	}
	wantFile := meas.BytesPerURL * 100e9
	if math.Abs(proj.MeguriFleetBytes-wantFile) > 1 {
		t.Fatalf(".meguri fleet projection %.0f does not match bytes/url x count %.0f", proj.MeguriFleetBytes, wantFile)
	}

	t.Logf("corpus: %d urls / %d hosts, %.2f bytes/url, %.2f bits/url @ fp %.4f, 0 false negatives",
		meas.URLs, meas.Hosts, meas.BytesPerURL, meas.BitsPerURL, meas.FPRate)
	t.Logf("projection: seen-set %s, .meguri %s", proj.SeenSetFleetCalc, proj.MeguriFleetCalc)
}

// TestPolitenessCurveOrders checks the curve machinery without a corpus: over a
// synthetic partition of mixed crawl delays the ceiling at k active hosts must be
// the sum of the k fastest hosts' rates (fastest first), monotone in k, clamped
// to the whole partition, and the full-partition point must equal the summed
// polite ceiling the walls report.
func TestPolitenessCurveOrders(t *testing.T) {
	// Delays in deciseconds: 5 (=2 fetch/s), 10 (=1), 20 (=0.5), 40 (=0.25).
	part := &format.Partition{Hosts: []m.HostRecord{
		{CrawlDelay: 20}, {CrawlDelay: 5}, {CrawlDelay: 40}, {CrawlDelay: 10},
	}}
	curve := PolitenessCurve(part, []int{1, 2, 3, 4, 99})

	// One active host is the single fastest (2 fetch/s), then the prefix sums add
	// the next-fastest each step: 2, 3, 3.5, 3.75.
	want := []struct {
		hosts   int
		ceiling float64
	}{{1, 2}, {2, 3}, {3, 3.5}, {4, 3.75}, {4, 3.75}}
	if len(curve) != len(want) {
		t.Fatalf("curve has %d points, want %d", len(curve), len(want))
	}
	for i, w := range want {
		if curve[i].ActiveHosts != w.hosts || math.Abs(curve[i].CeilingFPS-w.ceiling) > 1e-9 {
			t.Errorf("point %d = {%d hosts, %.3f}, want {%d, %.3f}", i, curve[i].ActiveHosts, curve[i].CeilingFPS, w.hosts, w.ceiling)
		}
	}
	for i := 1; i < len(curve); i++ {
		if curve[i].CeilingFPS < curve[i-1].CeilingFPS {
			t.Errorf("curve not monotone at %d: %.3f < %.3f", i, curve[i].CeilingFPS, curve[i-1].CeilingFPS)
		}
	}
}

// TestCorpusThroughputAnalysis is the doc 14 section 5.3 throughput gate on the
// real slice: the scheduler's selection rate is enormous next to the politeness
// ceiling the same host set imposes, so the partition is fetcher-bound and the
// only lever on throughput is more active hosts. It checks the analysis math
// against the partition's real crawl delays and logs the curve and the gap.
func TestCorpusThroughputAnalysis(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	part := loadCorpusPartition(t, path)
	if len(part.Hosts) == 0 {
		t.Skip("corpus partition carries no hosts with crawl delays")
	}

	// The measured scheduler selection rate from BenchmarkCorpusDispatchSelections;
	// a conservative floor well under the benchmark's observed sel/s so the gate
	// does not depend on a wall-clock measurement.
	const measuredSelFPS = 1e6
	thr := Analyze(part, measuredSelFPS)

	if thr.PoliteCeilingFPS <= 0 {
		t.Fatalf("polite ceiling is %.3f, want positive over %d hosts", thr.PoliteCeilingFPS, thr.ActiveHosts)
	}
	// The full-partition curve point is the summed ceiling the walls report.
	last := thr.Curve[len(thr.Curve)-1]
	if last.ActiveHosts != thr.ActiveHosts || math.Abs(last.CeilingFPS-thr.PoliteCeilingFPS) > 1e-6 {
		t.Errorf("curve tail {%d, %.3f} does not match the summed ceiling {%d, %.3f}", last.ActiveHosts, last.CeilingFPS, thr.ActiveHosts, thr.PoliteCeilingFPS)
	}
	// The fetcher-bound gap and the hosts-to-saturate count are the two derived
	// numbers; check they follow from the inputs.
	if math.Abs(thr.FetcherBoundGap-measuredSelFPS/thr.PoliteCeilingFPS) > 1e-3 {
		t.Errorf("fetcher-bound gap %.3f does not match sel/ceiling %.3f", thr.FetcherBoundGap, measuredSelFPS/thr.PoliteCeilingFPS)
	}
	// The scheduler outruns this slice's host set by a wide margin: a polite crawler
	// here is fetcher-bound, and it would take far more active hosts than the slice
	// holds to make the scheduler the bottleneck. That is the whole analysis.
	if !(thr.FetcherBoundGap > 1) {
		t.Errorf("scheduler not faster than the polite ceiling: gap %.3f", thr.FetcherBoundGap)
	}
	if !(thr.HostsToSaturate > float64(thr.ActiveHosts)) {
		t.Errorf("hosts-to-saturate %.0f is not above the slice's %d active hosts; the slice would already be scheduler-bound", thr.HostsToSaturate, thr.ActiveHosts)
	}

	t.Logf("throughput: scheduler %s sel/s vs polite ceiling %.1f fetch/s over %d hosts, fetcher-bound gap %sx",
		sci(thr.SchedulerFPS), thr.PoliteCeilingFPS, thr.ActiveHosts, sci(thr.FetcherBoundGap))
	t.Logf("throughput: %s active hosts would be needed to saturate the scheduler at this selection rate", sci(thr.HostsToSaturate))
	for _, p := range thr.Curve {
		t.Logf("  polite ceiling at %d active hosts: %.1f fetch/s", p.ActiveHosts, p.CeilingFPS)
	}
}

// TestNaiveFrontierBaseline checks the section-7 paired ratio: a naive exact-key
// frontier pays 128 bits/url and meguri's measured seen-set pays its bits/url, so
// the ratio is 128 over the measured cost and the fleet bytes follow the same
// multiplication as the projection. At 11 bits/url the tiered filter is ~11.6x
// smaller than an exact key set.
func TestNaiveFrontierBaseline(t *testing.T) {
	const total = 100e9
	bl := NaiveFrontierBaseline(Measured{BitsPerURL: 11}, total)

	if bl.NaiveBitsPerURL != 128 {
		t.Errorf("naive floor = %.0f bits/url, want 128", bl.NaiveBitsPerURL)
	}
	if math.Abs(bl.MemoryRatio-128.0/11.0) > 1e-9 {
		t.Errorf("memory ratio = %.4f, want %.4f", bl.MemoryRatio, 128.0/11.0)
	}
	wantNaive := 128.0 * total / 8
	if math.Abs(bl.NaiveFleetBytes-wantNaive) > 1 {
		t.Errorf("naive fleet = %.0f, want %.0f", bl.NaiveFleetBytes, wantNaive)
	}
	wantMeguri := 11.0 * total / 8
	if math.Abs(bl.MeguriFleetBytes-wantMeguri) > 1 {
		t.Errorf("meguri fleet = %.0f, want %.0f", bl.MeguriFleetBytes, wantMeguri)
	}
	// The naive store must come out larger, the whole point of the tiered filter.
	if !(bl.NaiveFleetBytes > bl.MeguriFleetBytes) {
		t.Errorf("naive fleet %.0f not larger than meguri %.0f", bl.NaiveFleetBytes, bl.MeguriFleetBytes)
	}
}

// TestCorpusNaiveBaseline pins the baseline ratio on the real slice: meguri's
// measured seen-set bits/url against the naive exact-key floor, so the paired
// ratio is reported on real data, not an assumed bits/url.
func TestCorpusNaiveBaseline(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	part := loadCorpusPartition(t, path)
	if len(part.URLs) < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000", len(part.URLs))
	}
	meas, err := Measure(part)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	bl := NaiveFrontierBaseline(meas, 100e9)
	if !(bl.MemoryRatio > 1) {
		t.Fatalf("tiered filter not smaller than the naive key set: ratio %.2f", bl.MemoryRatio)
	}
	t.Logf("baseline: meguri %.2f bits/url vs naive 128 bits/url = %.1fx smaller; %s", meas.BitsPerURL, bl.MemoryRatio, bl.Calc)
}

// TestBytesPerURLBudget is the section-8 per-commit budget guard: the .meguri
// bytes/url on the pinned slice must not grow past the tens-of-bytes target, so a
// new column or a worse default encoding that silently inflates the file fails
// the build. The ceiling is the raw-row floor; once a measured floor is recorded
// it tightens to that.
func TestBytesPerURLBudget(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	part := loadCorpusPartition(t, path)
	meas, err := Measure(part)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	// Measured ~25 bytes/url on CC-MAIN-2026-25 (the tens-of-bytes target, an order
	// of magnitude under the ~175-190 raw row). The guard holds the measured floor
	// with headroom so a real regression fails but encoding noise does not.
	const budget = 40.0
	if meas.BytesPerURL >= budget {
		t.Fatalf("bytes/url = %.2f, over the %.0f budget", meas.BytesPerURL, budget)
	}
}

// TestBitsPerURLBudget is the section-8 per-commit budget guard on the seen-set:
// bits/url at the achieved fp rate must not grow past the ~10-bit budget, because
// the hundred-gigabyte fleet projection rests on this number and a silent
// inflation would silently break it.
func TestBitsPerURLBudget(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	part := loadCorpusPartition(t, path)
	meas, err := Measure(part)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	const budget = 12.0 // ~10-bit target with blocking headroom
	if meas.BitsPerURL >= budget {
		t.Fatalf("seen-set bits/url = %.2f, over the %.0f budget", meas.BitsPerURL, budget)
	}
}

// sortURLs orders URL rows by URLKey, the order Encode verifies.
func sortURLs(urls []m.URLRecord) {
	for i := 1; i < len(urls); i++ {
		for j := i; j > 0 && urls[j].URLKey.Less(urls[j-1].URLKey); j-- {
			urls[j], urls[j-1] = urls[j-1], urls[j]
		}
	}
}

// itoa is the tiny base-10 helper the small-partition path keys with, avoiding a
// strconv import for one call.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
