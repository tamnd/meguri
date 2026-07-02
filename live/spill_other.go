//go:build !unix

package live

// openFileBudget reports a conservative open-file budget on platforms without a settable
// RLIMIT_NOFILE. The seal treats this as the ceiling for its per-shard spill and keeps
// the key set in memory when a large seal's shard files would not fit, so the fallback
// stays correct if slower on memory.
func openFileBudget() int { return 256 }
