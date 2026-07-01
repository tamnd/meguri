//go:build darwin || linux

package live

import (
	"os"

	"golang.org/x/sys/unix"
)

// mmapFile maps path read-only and returns the bytes plus a closer that unmaps
// them. The .meguri base is mapped, not read, because at 100M it is gigabytes and
// the goal box has 5 GiB: a read-only MAP_SHARED mapping is clean and file backed,
// so its resident pages are reclaimable page cache the kernel drops under pressure,
// never anonymous heap that the OOM killer counts (spec 2073 doc 08). The file
// handle is closed once the mapping is established; the mapping stays valid until
// munmap. An empty file maps to a nil slice with a no-op closer, since mmap rejects
// a zero length.
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
	return b, func() error { return unix.Munmap(b) }, nil
}

// adviseRandom tells the kernel the mapping is touched at random offsets (the
// point-lookup path), so it suppresses the readahead that would evict useful pages
// to prefetch pages around a fault that the next lookup will not want. A failure is
// not fatal: the advice is a hint, and the mapping works without it.
func adviseRandom(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = unix.Madvise(b, unix.MADV_RANDOM)
}

// adviseSequential tells the kernel the mapping is about to be scanned in order
// (the compaction read of the base), so it reads ahead and drops pages behind the
// cursor, keeping the scan's resident footprint a sliding window rather than the
// whole file.
func adviseSequential(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = unix.Madvise(b, unix.MADV_SEQUENTIAL)
}
