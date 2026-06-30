//go:build !darwin && !linux

package live

import "os"

// mmapFile falls back to a full read on platforms without the unix mmap path. The
// fleet runs on linux and macOS where mmap_unix.go provides the page-cache mapping;
// this keeps the package building elsewhere at the cost of a resident copy of the
// base file, which is fine for a small file and not the goal box.
func mmapFile(path string) ([]byte, func() error, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return b, func() error { return nil }, nil
}

func adviseRandom([]byte)     {}
func adviseSequential([]byte) {}
