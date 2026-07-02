//go:build darwin || linux

package seed

import (
	"os"

	"golang.org/x/sys/unix"
)

// mmapFile maps path read-only and returns the bytes plus an unmap closer. The seed
// is read whole-block and sequentially, so a MAP_SHARED mapping keeps its pages as
// reclaimable page cache rather than heap, the same reason the .meguri base is
// mapped and not read (live/mmap_unix.go). An empty file maps to nil with a no-op
// closer, since mmap rejects a zero length.
func mmapFile(path string) ([]byte, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	n := fi.Size()
	if n == 0 {
		return nil, func() error { return nil }, nil
	}
	b, err := unix.Mmap(int(f.Fd()), 0, int(n), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	_ = unix.Madvise(b, unix.MADV_SEQUENTIAL)
	return b, func() error { return unix.Munmap(b) }, nil
}
