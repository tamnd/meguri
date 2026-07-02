// Package scale is the timed, resource-instrumented sibling of the bench package.
//
// bench measures the deterministic, box-independent facts of a partition: bytes
// per URL, seen-set bits per URL, the achieved false-positive rate, rebalance
// bytes. Those numbers are exact and need no clock. scale measures the other half,
// the numbers a clock and a machine produce: wall and CPU time per stage, peak
// resident memory and heap, allocations per URL, disk bytes and fsync, and the
// throughput each stage sustains. It drives the real engine entry points (the same
// frontier.Seed and engine.Run the CLI calls), so the numbers are the engine's own
// numbers, not a reimplementation's.
//
// Every timed number this package emits is only as trustworthy as the box it ran
// on, so a Result carries its provenance (box label, commit, corpus) and the
// harness refuses to stamp a run "measured" without a box label and a real corpus.
// The honesty rule from Spec 2071 doc 14 and Spec scale doc 00 holds here unchanged.
package scale

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sort"
	"time"
)

// Provenance is the stamp every measured number carries. A number without all of
// these is not admissible to the spec tables (scale doc 10).
type Provenance struct {
	Box    string `json:"box"`    // the machine label, the box of record
	Commit string `json:"commit"` // the meguri commit the run was built from
	Corpus string `json:"corpus"` // the pinned corpus path or name
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
	NumCPU int    `json:"num_cpu"`
}

// MemSummary is the memory side of a stage: the peak resident set the OS charged
// the process (getrusage max-rss, the real ceiling), the peak Go heap in use, the
// bytes and objects allocated over the stage, and the GC work those allocations
// drove. PeakRSS is the metric of record for the memory ceiling because it includes
// stacks, the runtime, and mmap, not just the heap.
type MemSummary struct {
	PeakRSSBytes    uint64  `json:"peak_rss_bytes"`
	PeakHeapInUse   uint64  `json:"peak_heap_inuse_bytes"`
	HeldHeapInUse   uint64  `json:"held_heap_inuse_bytes,omitempty"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	Mallocs         uint64  `json:"mallocs"`
	NumGC           uint32  `json:"num_gc"`
	GCPauseTotalNs  uint64  `json:"gc_pause_total_ns"`
	GCCPUFraction   float64 `json:"gc_cpu_fraction"`
}

// HeldHeap measures the live heap an object holds after a forced GC, the resident
// footprint that survives between operations rather than the transient peak a
// one-shot encode spikes to. The seed stage uses it to separate the held frontier
// (the number the residency budget caps) from the checkpoint-encode high-water the
// peak RSS captures. It forces a GC so only reachable bytes remain, then reads the
// heap in-use. ref keeps the measured object alive across the GC so it is not
// collected before the read.
func HeldHeap(ref any) uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(ref)
	return ms.HeapInuse
}

// CPUTime is the user and system CPU a stage burned, read from getrusage deltas.
// CPU-seconds, not wall, is the throughput metric of record (doc 14 section 2.3):
// wall moves with machine load, user CPU does not.
type CPUTime struct {
	UserSeconds float64 `json:"user_seconds"`
	SysSeconds  float64 `json:"sys_seconds"`
}

// DiskSummary is the byte and durability side of a stage: bytes written and read
// through the harness's counting wrappers, the fsync count, and the output file
// size. fsync latency is captured by the SyncTimer when the stage flushes.
type DiskSummary struct {
	BytesWritten uint64  `json:"bytes_written"`
	BytesRead    uint64  `json:"bytes_read"`
	OutputBytes  uint64  `json:"output_bytes"`
	FsyncCount   uint64  `json:"fsync_count"`
	FsyncP50Ms   float64 `json:"fsync_p50_ms"`
	FsyncP99Ms   float64 `json:"fsync_p99_ms"`
}

// IOSummary is the kernel-charged IO side of a stage, read as a getrusage delta.
// MajorFaults is the load-bearing metric for the file-backed engine: a major fault
// is a page the mmap read pulled off the disk, so its per-stage delta is that stage's
// real read-IO, the same figure /usr/bin/time labels "Major (requiring I/O) page
// faults". MinorFaults are served from the page cache with no disk touch, so the
// major/minor ratio reads how well the working set fits cache. BlockIn/BlockOut are
// the process's charged block-layer operations; on Linux they read zero for page-cache
// and mmap traffic (the block accounting is charged at fault time, not to the process),
// so MajorFaults, not BlockIn, is the mmap read signal to trust.
type IOSummary struct {
	MajorFaults uint64 `json:"major_faults"`
	MinorFaults uint64 `json:"minor_faults"`
	BlockIn     uint64 `json:"block_in"`
	BlockOut    uint64 `json:"block_out"`
}

// LatencySummary is the structured form of a per-op latency histogram: the percentile
// edges and the max, in nanoseconds, plus the sample count and the engine-only rate
// (count over summed observed durations, isolating the measured op from the corpus
// parse the stage wall also includes). The command already renders these into the
// stage Notes; this carries the same figures machine-readably into the ledger JSON so
// a regression check can read p99 without parsing prose. Percentiles are the
// log2-bucket upper edge, so they are order-of-magnitude exact, which is what a
// hundreds-of-nanoseconds per-op latency needs.
type LatencySummary struct {
	Op            string  `json:"op"` // the measured operation, e.g. PutURL, Seen, NextBatch
	Samples       uint64  `json:"samples"`
	P50Ns         uint64  `json:"p50_ns"`
	P90Ns         uint64  `json:"p90_ns"`
	P99Ns         uint64  `json:"p99_ns"`
	MaxNs         uint64  `json:"max_ns"`
	EngineOpsPerS float64 `json:"engine_ops_per_second"`
}

// StageResult is one measured pipeline stage: what it processed, how long it took
// in wall and CPU, the memory it cost, the disk and kernel IO it touched, the per-op
// latency where the stage has a hot op, and the throughput that implies. Derived
// per-URL numbers (alloc/url, bytes/url) are computed from the counts and the URL
// total so a reader does not recompute them. Network has no field: the scale path is
// offline (the seed is a local .seed and no stage fetches), so there is no network IO
// to measure and an empty field would only imply otherwise.
type StageResult struct {
	Stage            string          `json:"stage"`
	URLs             int             `json:"urls"`
	WallSeconds      float64         `json:"wall_seconds"`
	CPU              CPUTime         `json:"cpu"`
	Mem              MemSummary      `json:"mem"`
	Disk             DiskSummary     `json:"disk"`
	IO               IOSummary       `json:"io,omitzero"`         // kernel-charged faults and block ops, the read-IO term
	URLsPerSecond    float64         `json:"urls_per_second"`     // count / wall, paired with CPU below
	URLsPerCPUSec    float64         `json:"urls_per_cpu_second"` // count / user CPU, the load-stable rate
	AllocBytesPerURL float64         `json:"alloc_bytes_per_url"`
	RSS              RSSSplit        `json:"rss_split,omitzero"` // anon/file resident split, the doc 08 residency term
	Latency          *LatencySummary `json:"latency,omitempty"`  // per-op histogram where the stage has a hot op
	Notes            string          `json:"notes,omitempty"`
}

// Result aggregates a whole scale run: its provenance, the profile it ran, and one
// StageResult per pipeline stage. It is the immutable ledger entry (scale doc 10)
// and marshals straight to the JSON the regression ledger consumes.
type Result struct {
	Profile    string        `json:"profile"`
	Provenance Provenance    `json:"provenance"`
	StartedAt  string        `json:"started_at"`
	Stages     []StageResult `json:"stages"`
	PprofDir   string        `json:"pprof_dir,omitempty"`
}

// requireMeasurable enforces the honesty rule: a Result is only admissible as
// "measured" when it names a box and a real corpus. A run with neither is a smoke
// run, useful for catching breakage, never a number of record.
func (r Result) requireMeasurable() error {
	if r.Provenance.Box == "" {
		return fmt.Errorf("scale: refusing to stamp measured without --box (the box of record)")
	}
	if r.Provenance.Corpus == "" {
		return fmt.Errorf("scale: refusing to stamp measured without a real corpus")
	}
	return nil
}

// CountingWriter tallies the bytes written through it, the disk-accounting wrapper
// the harness wraps a checkpoint writer in so the write path's byte cost is the
// engine's real output, not an estimate.
type CountingWriter struct {
	W io.Writer
	N uint64
}

func (c *CountingWriter) Write(p []byte) (int, error) {
	n, err := c.W.Write(p)
	c.N += uint64(n)
	return n, err
}

// SyncTimer records fsync latencies so a stage can report fsync count and the p50
// and p99 of the flush. The device fsync floor is a property of the disk, measured
// on the box of record, not a number the engine chooses; this only times the calls
// the engine actually makes.
type SyncTimer struct {
	latencies []time.Duration
}

// Time runs sync and records how long it took.
func (s *SyncTimer) Time(sync func() error) error {
	start := time.Now()
	err := sync()
	s.latencies = append(s.latencies, time.Since(start))
	return err
}

// Summary returns the fsync count and the p50 and p99 latency in milliseconds.
func (s *SyncTimer) Summary() (count uint64, p50ms, p99ms float64) {
	n := len(s.latencies)
	if n == 0 {
		return 0, 0, 0
	}
	d := make([]time.Duration, n)
	copy(d, s.latencies)
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	pick := func(q float64) float64 {
		idx := int(q * float64(n-1))
		return float64(d[idx]) / float64(time.Millisecond)
	}
	return uint64(n), pick(0.50), pick(0.99)
}

// Sampler watches the Go heap on a ticker so a stage can report its peak heap in
// use, not just the heap at the end. It runs in its own goroutine between Start and
// Stop. Peak RSS comes from getrusage (the OS view, in measure.go), not from here;
// this catches the in-flight heap high-water mark a single end-of-stage read misses.
type Sampler struct {
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	peakHeap uint64
}

// NewSampler builds a heap sampler that reads every interval.
func NewSampler(interval time.Duration) *Sampler {
	return &Sampler{interval: interval, stop: make(chan struct{}), done: make(chan struct{})}
}

// Start begins sampling in a background goroutine.
func (s *Sampler) Start() {
	go func() {
		defer close(s.done)
		t := time.NewTicker(s.interval)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > s.peakHeap {
					s.peakHeap = ms.HeapInuse
				}
			}
		}
	}()
}

// Stop ends sampling and returns the peak heap-in-use it observed.
func (s *Sampler) Stop() uint64 {
	close(s.stop)
	<-s.done
	return s.peakHeap
}

// WriteJSON marshals the result as indented JSON to w, the machine-readable ledger
// entry. The same bytes go to scale-results/ and feed the doc 14 As-Built table.
func (r Result) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteHuman prints a readable per-stage summary, the form an operator reads at the
// terminal. The honesty discipline shows in the header: a run without a box says so.
func (r Result) WriteHuman(w io.Writer) {
	stamp := r.Provenance.Box
	if stamp == "" {
		stamp = "SMOKE (no box, not a number of record)"
	}
	fmt.Fprintf(w, "scale run: profile=%s box=%s commit=%s corpus=%s\n",
		r.Profile, stamp, r.Provenance.Commit, r.Provenance.Corpus)
	fmt.Fprintf(w, "  host: %s/%s, %d cpu\n", r.Provenance.GOOS, r.Provenance.GOARCH, r.Provenance.NumCPU)
	for _, s := range r.Stages {
		fmt.Fprintf(w, "\nstage %s: %d urls\n", s.Stage, s.URLs)
		fmt.Fprintf(w, "  wall            %.4f s\n", s.WallSeconds)
		fmt.Fprintf(w, "  cpu             %.4f s user, %.4f s sys\n", s.CPU.UserSeconds, s.CPU.SysSeconds)
		fmt.Fprintf(w, "  throughput      %s urls/s wall, %s urls/cpu-s\n",
			human(s.URLsPerSecond), human(s.URLsPerCPUSec))
		fmt.Fprintf(w, "  peak rss        %s\n", humanBytes(s.Mem.PeakRSSBytes))
		if s.RSS.Available {
			perURL := ""
			if s.URLs > 0 {
				perURL = fmt.Sprintf(", %.2f anon B/url", float64(s.RSS.AnonBytes)/float64(s.URLs))
			}
			fmt.Fprintf(w, "  rss split       %s anon, %s file, %s shmem (vmrss %s)%s\n",
				humanBytes(s.RSS.AnonBytes), humanBytes(s.RSS.FileBytes),
				humanBytes(s.RSS.ShmemBytes), humanBytes(s.RSS.VMRSSBytes), perURL)
		}
		fmt.Fprintf(w, "  peak heap       %s\n", humanBytes(s.Mem.PeakHeapInUse))
		if s.Mem.HeldHeapInUse > 0 {
			held := s.Mem.HeldHeapInUse
			perURL := ""
			if s.URLs > 0 {
				perURL = fmt.Sprintf(", %.1f bytes/url", float64(held)/float64(s.URLs))
			}
			fmt.Fprintf(w, "  held heap       %s%s\n", humanBytes(held), perURL)
		}
		fmt.Fprintf(w, "  alloc/url       %.1f bytes (%s total, %d objects)\n",
			s.AllocBytesPerURL, humanBytes(s.Mem.TotalAllocBytes), s.Mem.Mallocs)
		fmt.Fprintf(w, "  gc              %d cycles, %.2f ms pause total, %.4f cpu fraction\n",
			s.Mem.NumGC, float64(s.Mem.GCPauseTotalNs)/1e6, s.Mem.GCCPUFraction)
		if s.Disk.OutputBytes > 0 || s.Disk.BytesWritten > 0 {
			fmt.Fprintf(w, "  disk            %s written, %s output file\n",
				humanBytes(s.Disk.BytesWritten), humanBytes(s.Disk.OutputBytes))
		}
		if s.Disk.FsyncCount > 0 {
			fmt.Fprintf(w, "  fsync           %d calls, p50 %.3f ms, p99 %.3f ms\n",
				s.Disk.FsyncCount, s.Disk.FsyncP50Ms, s.Disk.FsyncP99Ms)
		}
		if s.Notes != "" {
			fmt.Fprintf(w, "  note            %s\n", s.Notes)
		}
	}
}

func human(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2fG", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.2fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func humanBytes(b uint64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
