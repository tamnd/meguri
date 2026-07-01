//go:build unix

package scale

import (
	"syscall"
	"time"
)

// readRusage reads the process getrusage on Unix: cumulative user/system CPU, the
// kernel's peak resident-set high-water mark (converted to bytes by the platform
// maxRSSBytes: ru_maxrss is bytes on darwin, kibibytes on linux), and the page-fault
// and block-IO counters. Majflt is the metric of record for the file-backed engine:
// a major fault is a page the mmap read had to pull off the disk, so its delta across
// a stage is that stage's real read-IO, the number /usr/bin/time reports as
// "Major (requiring I/O) page faults".
func readRusage() rusageSnapshot {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return rusageSnapshot{
		user:    timevalDur(ru.Utime),
		sys:     timevalDur(ru.Stime),
		rss:     maxRSSBytes(ru.Maxrss),
		majflt:  uint64(ru.Majflt),
		minflt:  uint64(ru.Minflt),
		inblock: uint64(ru.Inblock),
		oublock: uint64(ru.Oublock),
	}
}

func timevalDur(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}
