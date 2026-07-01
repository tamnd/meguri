//go:build windows

package scale

import (
	"time"

	"golang.org/x/sys/windows"
)

// readRusage reads the process resource view on Windows. CPU comes from
// GetProcessTimes (kernel + user FILETIME, each in 100 ns ticks). RSS is left zero:
// the high-water resident set is not read here (it needs the psapi process-memory
// call the pinned x/sys does not expose), and the harness's load-bearing memory
// metric is the Go-runtime peak heap from the sampler, which is platform-independent.
// This mirrors the Unix getrusage path closely enough that the scale harness reports
// the same CPU and throughput fields on the 64 GB Windows box of record.
func readRusage() rusageSnapshot {
	h := windows.CurrentProcess()
	var creation, exit, kernel, user windows.Filetime
	_ = windows.GetProcessTimes(h, &creation, &exit, &kernel, &user)
	return rusageSnapshot{
		user: filetimeDur(user),
		sys:  filetimeDur(kernel),
		rss:  0,
	}
}

// filetimeDur converts a FILETIME (100 ns ticks since process start for the CPU
// counters) to a Duration.
func filetimeDur(ft windows.Filetime) time.Duration {
	ticks := uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime)
	return time.Duration(ticks) * 100 * time.Nanosecond
}
