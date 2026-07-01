//go:build !darwin && !linux

package scale

// maxRSSBytes is the fallback for platforms whose ru_maxrss unit the harness does
// not special-case. It returns 0 so a reader sees "not captured" rather than a
// wrong number; the boxes of record are Linux and the dev box is darwin.
func maxRSSBytes(maxrss int64) uint64 { return 0 }
