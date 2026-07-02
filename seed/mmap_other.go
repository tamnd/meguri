//go:build !darwin && !linux

package seed

import "os"

// mmapFile falls back to a full read on platforms without the unix mmap path, so
// the seed reader still works off-box for tests and tooling. The scale box is
// linux, where the real mmap path runs.
func mmapFile(path string) ([]byte, func() error, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return b, func() error { return nil }, nil
}
