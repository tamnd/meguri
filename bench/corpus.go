package bench

import "fmt"

// The benchmark corpus is a frozen Common Crawl slice (CC-MAIN-2026-25, the seed
// domains in corpus/MANIFEST). M1 through M5 seeded it by domain: the gates loaded
// every captured URL and never asked which slice of the host-key space those
// domains land in. At 100B pages a benchmark cannot enumerate domains; it pins a
// host-key range and crawls the hosts whose key falls in it, the way the fleet
// router slices ownership by host-key range (the HostKeyLo/HostKeyHi a partition's
// .meguri header and the partition map both carry). Pinning the range makes the
// benchmark slice reproducible as a key interval, not a domain list, and ties it to
// the jump-hash ownership math the fleet runs at scale (doc 12, doc 14, audit 288).
//
// The pinned bounds below are the min and max HostKeyOf over the frozen corpus's
// distinct hosts, computed once and frozen here. A gate recomputes the range from
// corpus/urls.jsonl and asserts it still matches, so a corpus that drifts off the
// pin fails loudly rather than silently re-baselining a measurement.
const (
	// PinnedCorpusHostKeyLo and PinnedCorpusHostKeyHi bound the host-key range the
	// frozen corpus covers: the lowest and highest meguri.HostKeyOf over its
	// distinct hosts. Every corpus host-key falls in [lo, hi].
	PinnedCorpusHostKeyLo uint64 = 0x094047007b11482a
	PinnedCorpusHostKeyHi uint64 = 0xbd81fc1c46f41981
	// PinnedCorpusHosts is the count of distinct hosts the range spans, the host
	// groups the corpus seeds resolve to.
	PinnedCorpusHosts int = 11
)

// HostKeyRange is the host-key interval a corpus slice covers, the range-pinned
// form of the benchmark seed set. Lo and Hi are the inclusive bounds; Hosts is the
// distinct host count inside them.
type HostKeyRange struct {
	Lo    uint64
	Hi    uint64
	Hosts int
}

// CorpusHostKeyRange computes the host-key range over a set of distinct host keys:
// the min and max key and the host count. It is how the gate derives the live range
// from the corpus to check it against the pin. An empty input yields a zero range.
func CorpusHostKeyRange(hostKeys []uint64) HostKeyRange {
	if len(hostKeys) == 0 {
		return HostKeyRange{}
	}
	lo, hi := hostKeys[0], hostKeys[0]
	for _, k := range hostKeys[1:] {
		lo = min(lo, k)
		hi = max(hi, k)
	}
	return HostKeyRange{Lo: lo, Hi: hi, Hosts: len(hostKeys)}
}

// Contains reports whether a host-key falls inside the range, the test a fleet
// partition runs to decide it owns a host's URLs.
func (r HostKeyRange) Contains(hostKey uint64) bool {
	return hostKey >= r.Lo && hostKey <= r.Hi
}

// SpanFraction is the share of the full 64-bit host-key space the range covers, a
// rough gauge of how much of the fleet's key space the corpus slice exercises. The
// frozen corpus spans a wide interval because its seed hosts scatter across the
// hash, so the fraction is near one even though only a handful of hosts land in it.
func (r HostKeyRange) SpanFraction() float64 {
	if r.Hi <= r.Lo {
		return 0
	}
	return float64(r.Hi-r.Lo) / float64(^uint64(0))
}

// PinnedRange returns the frozen host-key range the gate checks the corpus against.
func PinnedRange() HostKeyRange {
	return HostKeyRange{Lo: PinnedCorpusHostKeyLo, Hi: PinnedCorpusHostKeyHi, Hosts: PinnedCorpusHosts}
}

// HostKeyRangeReport renders the pinned range for the bench command, the methodology
// line that records the benchmark corpus is a host-key interval, not a domain list.
func HostKeyRangeReport(r HostKeyRange) string {
	return fmt.Sprintf("corpus host-key range: [0x%016x, 0x%016x] over %d hosts, %.4f of the key space",
		r.Lo, r.Hi, r.Hosts, r.SpanFraction())
}
