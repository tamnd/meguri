package scale

// RSSSplit is the resident-set breakdown the file-backed engine is measured
// against (spec 2073 doc 08). The whole residency claim of the mmap design is that
// the multi-gigabyte .meguri file maps as reclaimable file-backed page cache, not
// anonymous heap, so the OOM budget is the anonymous part alone. Capturing only
// the total VmRSS reads a healthy file-backed run as a budget blowout, because the
// mapped file counts toward VmRSS but is evictable under pressure. AnonBytes is
// the number that must stay under the box budget; FileBytes is the reclaimable
// page cache the mapped base occupies and is expected to be large.
type RSSSplit struct {
	VMRSSBytes uint64 `json:"vm_rss_bytes"`
	AnonBytes  uint64 `json:"rss_anon_bytes"`
	FileBytes  uint64 `json:"rss_file_bytes"`
	ShmemBytes uint64 `json:"rss_shmem_bytes"`
	Available  bool   `json:"available"`
}
