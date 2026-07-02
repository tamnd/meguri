package scale

import (
	"runtime"
	"time"
)

// rusageSnapshot is the OS resource view at a moment: cumulative user and system
// CPU and the peak resident set so far. CPU is read as a delta across a stage; RSS
// is a high-water mark the kernel maintains, so the value after a stage is the peak
// the process reached during it. readRusage is supplied per platform (measure_unix.go
// via getrusage, measure_windows.go via the process API).
type rusageSnapshot struct {
	user    time.Duration
	sys     time.Duration
	rss     uint64
	majflt  uint64 // major page faults: the ones that hit the disk, the mmap-read signal
	minflt  uint64 // minor page faults: served from the page cache, no disk
	inblock uint64 // block input operations charged to the process
	oublock uint64 // block output operations charged to the process
}

// StageResultFromSeed measures a seed-type stage: fn builds the frontier and
// returns the checkpoint bytes it wrote, and urls is the input URL count, the
// denominator for the intake throughput and alloc-per-URL numbers.
func StageResultFromSeed(urls int, fn func() (written uint64, err error)) (StageResult, error) {
	return stageMetrics("seed", urls, fn)
}

// StageResultFromRun measures a run-type stage: fn drives the engine drain loop and
// urls is the resident URL count it drained.
func StageResultFromRun(urls int, fn func() (written uint64, err error)) (StageResult, error) {
	return stageMetrics("run", urls, fn)
}

// StageResultFromIngest measures an ingest-type stage: fn drives the durable
// store path with a resident budget, building and writing one record per URL so
// the cold bulk spills to the log rather than staying resident. urls is the input
// URL count, the denominator for ingest throughput and the bytes-on-disk ratio.
func StageResultFromIngest(urls int, fn func() (written uint64, err error)) (StageResult, error) {
	return stageMetrics("ingest", urls, fn)
}

// StageResultFromLive measures a live-engine stage: fn drives the file-backed
// engine (a bulk load that writes one .meguri, or a dedup/lookup pass over the
// mapped file), returning the file bytes it produced or touched. urls is the URL
// count fn processed. The caller stamps the anon/file RSS split onto the result,
// the residency term the mmap design is judged on (spec 2073 doc 08).
func StageResultFromLive(urls int, fn func() (bytes uint64, err error)) (StageResult, error) {
	return stageMetrics("live", urls, fn)
}

// StageResultFromInspect measures an inspect-type stage: fn reads a .meguri
// checkpoint off disk and decodes its columns, returning the bytes it read.
// urls is the URL count the decode reconstructed, the denominator for the
// decode throughput. The bytes are accounted as read, not written, so this is
// the one stage that fills the disk read side of the ledger.
func StageResultFromInspect(urls int, fn func() (read uint64, err error)) (StageResult, error) {
	res, err := stageMetrics("inspect", urls, fn)
	if err != nil {
		return res, err
	}
	read := res.Disk.BytesWritten
	res.Disk = DiskSummary{BytesRead: read}
	return res, nil
}

// WithURLs restamps a stage's URL denominator and recomputes the per-URL and
// throughput fields from it. It is for a stage that does not know its work count
// until fn has run (an inspect decode learns the URL count only after decoding),
// so it measures with urls=0 and restamps the real count here. Wall, CPU, and
// allocation totals are unchanged; only the denominated ratios are recomputed.
func WithURLs(res StageResult, urls int) StageResult {
	res.URLs = urls
	res.URLsPerSecond = 0
	res.URLsPerCPUSec = 0
	res.AllocBytesPerURL = 0
	if urls > 0 {
		if res.WallSeconds > 0 {
			res.URLsPerSecond = float64(urls) / res.WallSeconds
		}
		if res.CPU.UserSeconds > 0 {
			res.URLsPerCPUSec = float64(urls) / res.CPU.UserSeconds
		}
		res.AllocBytesPerURL = float64(res.Mem.TotalAllocBytes) / float64(urls)
	}
	return res
}

// stageMetrics runs fn as a measured stage and returns its StageResult. It pins the
// goroutine, forces a GC so the heap baseline is clean, snapshots getrusage and
// MemStats, runs fn under a heap sampler, then snapshots again and differences the
// counters. urls is the work count fn processed, the denominator for the per-URL
// and throughput numbers. fn returns the bytes it wrote (for disk accounting) so a
// stage that produces a checkpoint reports its real output size.
func stageMetrics(stage string, urls int, fn func() (written uint64, err error)) (StageResult, error) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	ruBefore := readRusage()

	sampler := NewSampler(20 * time.Millisecond)
	sampler.Start()
	wallStart := time.Now()
	written, err := fn()
	wall := time.Since(wallStart)
	peakHeap := sampler.Stop()
	if err != nil {
		return StageResult{Stage: stage}, err
	}

	runtime.ReadMemStats(&after)
	ruAfter := readRusage()

	res := StageResult{
		Stage:       stage,
		URLs:        urls,
		WallSeconds: wall.Seconds(),
		CPU: CPUTime{
			UserSeconds: (ruAfter.user - ruBefore.user).Seconds(),
			SysSeconds:  (ruAfter.sys - ruBefore.sys).Seconds(),
		},
		Mem: MemSummary{
			PeakRSSBytes:    ruAfter.rss,
			PeakHeapInUse:   peakHeap,
			TotalAllocBytes: after.TotalAlloc - before.TotalAlloc,
			Mallocs:         after.Mallocs - before.Mallocs,
			NumGC:           after.NumGC - before.NumGC,
			GCPauseTotalNs:  after.PauseTotalNs - before.PauseTotalNs,
			GCCPUFraction:   after.GCCPUFraction,
		},
		Disk: DiskSummary{
			BytesWritten: written,
			OutputBytes:  written,
		},
		IO: IOSummary{
			MajorFaults: ruAfter.majflt - ruBefore.majflt,
			MinorFaults: ruAfter.minflt - ruBefore.minflt,
			BlockIn:     ruAfter.inblock - ruBefore.inblock,
			BlockOut:    ruAfter.oublock - ruBefore.oublock,
		},
	}
	if peakHeap < after.HeapInuse {
		res.Mem.PeakHeapInUse = after.HeapInuse
	}
	if wall.Seconds() > 0 {
		res.URLsPerSecond = float64(urls) / wall.Seconds()
	}
	if cpu := res.CPU.UserSeconds; cpu > 0 {
		res.URLsPerCPUSec = float64(urls) / cpu
	}
	if urls > 0 {
		res.AllocBytesPerURL = float64(res.Mem.TotalAllocBytes) / float64(urls)
	}
	return res, nil
}
