//go:build unix

package live

import "syscall"

// openFileBudget reports how many files the process may hold open, best-effort raising
// the soft RLIMIT_NOFILE to the hard cap first so a large seal's per-shard spill fits.
// The seal uses this to decide between spilling the key set to disk and holding it in
// memory: if the shard files will not fit, it keeps the in-memory shards that answer
// identically. Root can raise the soft limit to the hard limit, which is what the 100M
// runs need for the 512-shard spill.
func openFileBudget() int {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 256
	}
	if lim.Cur < lim.Max {
		raised := lim
		raised.Cur = lim.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &raised); err == nil {
			lim = raised
		} else if e := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); e != nil {
			return 256
		}
	}
	const cap = 1 << 20
	if lim.Cur > cap {
		return cap
	}
	return int(lim.Cur)
}
