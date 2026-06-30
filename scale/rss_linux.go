//go:build linux

package scale

// maxRSSBytes converts getrusage Maxrss to bytes. On Linux ru_maxrss is in
// kilobytes, so it is scaled up. The fleet box of record is Linux, so this is the
// path the memory numbers of record run through.
func maxRSSBytes(maxrss int64) uint64 {
	if maxrss < 0 {
		return 0
	}
	return uint64(maxrss) * 1024
}
