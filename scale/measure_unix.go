//go:build unix

package scale

import (
	"syscall"
	"time"
)

// readRusage reads the process getrusage on Unix: cumulative user/system CPU and the
// kernel's peak resident-set high-water mark, the latter converted to bytes by the
// platform maxRSSBytes (ru_maxrss is bytes on darwin, kibibytes on linux).
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
