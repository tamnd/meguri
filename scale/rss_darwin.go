//go:build darwin

package scale

// maxRSSBytes converts getrusage Maxrss to bytes. On darwin ru_maxrss is already
// in bytes, so it passes through.
func maxRSSBytes(maxrss int64) uint64 {
	if maxrss < 0 {
		return 0
	}
	return uint64(maxrss)
}
