package scale

import (
	"runtime"
	"syscall"
	"time"
)

// rusageSnapshot is the OS resource view at a moment: cumulative user and system
// CPU and the peak resident set so far. CPU is read as a delta across a stage; RSS
// is a high-water mark the kernel maintains, so the value after a stage is the peak
// the process reached during it.
type rusageSnapshot struct {
	user time.Duration
	sys  time.Duration
	rss  uint64
}

func readRusage() rusageSnapshot {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return rusageSnapshot{
		user: timevalDur(ru.Utime),
		sys:  timevalDur(ru.Stime),
		rss:  maxRSSBytes(ru.Maxrss),
	}
}

func timevalDur(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
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
