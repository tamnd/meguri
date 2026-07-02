//go:build linux

package scale

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// ReadRSSSplit reads the anon/file resident split from /proc/self/status. The
// fleet boxes of record are Linux, so this is the path the doc 08 residency
// numbers run through: RssAnon is the heap-and-stack term the box budget caps,
// RssFile is the mapped .meguri page cache the kernel reclaims first. The fields
// are reported in kibibytes, so each is scaled to bytes.
func ReadRSSSplit() RSSSplit {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return RSSSplit{}
	}
	defer func() { _ = f.Close() }()

	var s RSSSplit
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		field, ok := strings.CutSuffix(line, " kB")
		if !ok {
			continue
		}
		key, rest, ok := strings.Cut(field, ":")
		if !ok {
			continue
		}
		val := strings.TrimSpace(rest)
		kb, perr := strconv.ParseUint(val, 10, 64)
		if perr != nil {
			continue
		}
		bytes := kb * 1024
		switch key {
		case "VmRSS":
			s.VMRSSBytes = bytes
		case "RssAnon":
			s.AnonBytes = bytes
		case "RssFile":
			s.FileBytes = bytes
		case "RssShmem":
			s.ShmemBytes = bytes
		}
	}
	s.Available = s.VMRSSBytes > 0
	return s
}
