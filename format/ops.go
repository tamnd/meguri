package format

import (
	"sort"

	m "github.com/tamnd/meguri"
)

// This file is the partition-file maintenance set (doc 10 sections 11 and 12):
// pack a partition into a file, compact a file by dropping dead rows and
// reclaiming the string arena, split a partition at a HostKey boundary, and
// merge two adjacent partitions into one. Split and merge are the rebalance
// primitives a partition move uses (doc 12); compact is the garbage collector
// that reclaims the space tombstoned and Gone rows leave behind.
//
// All four work on the in-memory Partition, the unit Encode writes and Decode
// reads, so a caller composes them with Encode/Decode at the file boundary: load
// a file, split it, encode the two halves, write them, update the manifest. They
// keep the two invariants the format rests on: the URL rows stay sorted by
// URLKey, and a host's rows stay contiguous, so a split never cuts a host in two
// and a merge never interleaves hosts.

// Pack builds a Partition from already-sorted records and the shared string
// arena. It is the in-memory step before Encode: a caller that has live URL and
// host records hands them here, sets the partition identity, and gets a value
// ready to serialize. Pack does not sort; it trusts the caller, the same
// contract Encode keeps, so an unsorted input is caught at Encode.
func Pack(id uint32, hostKeyLo, hostKeyHi uint64, createdHours uint32, codec uint8, urls []m.URLRecord, hosts []m.HostRecord, strings []byte) *Partition {
	return &Partition{
		ID:           id,
		HostKeyLo:    hostKeyLo,
		HostKeyHi:    hostKeyHi,
		CreatedHours: createdHours,
		DefaultCodec: codec,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      strings,
	}
}

// Compact rewrites a partition dropping the URL rows whose status is Gone (the
// crawl has confirmed they no longer exist, doc 03) and reclaiming the string
// arena to only the spans the surviving rows still reference. A partition that
// has accumulated Gone rows and orphaned strings shrinks to exactly its live
// footprint, which is the garbage collection D11's checkpoint rotation leans on.
// The result is a fresh Partition; the input is untouched.
func Compact(p *Partition) *Partition {
	live := make([]m.URLRecord, 0, len(p.URLs))
	for _, r := range p.URLs {
		if r.Status == m.StatusGone {
			continue
		}
		live = append(live, r)
	}
	out := &Partition{
		ID:           p.ID,
		HostKeyLo:    p.HostKeyLo,
		HostKeyHi:    p.HostKeyHi,
		CreatedHours: p.CreatedHours,
		DefaultCodec: p.DefaultCodec,
		URLs:         live,
		Hosts:        append([]m.HostRecord(nil), p.Hosts...),
		Meta:         cloneMeta(p.Meta),
	}
	rebaseArena(out, p.Strings)
	return out
}

// Split divides a partition into a low half owning HostKeys < atHostKey and a
// high half owning HostKeys >= atHostKey. Because the URL rows are sorted by
// URLKey (HostKey high half first) and a host's rows are contiguous, the cut is
// one boundary in each table, so neither half ever holds part of a host. Each
// half gets its own re-based string arena holding only the spans its rows
// reference. The two halves carry the same partition id and creation time as the
// input; a caller assigns new identities and ranges as the rebalance needs.
func Split(p *Partition, atHostKey uint64) (lo, hi *Partition) {
	ui := sort.Search(len(p.URLs), func(i int) bool { return p.URLs[i].URLKey.HostKey >= atHostKey })
	hi0 := sort.Search(len(p.Hosts), func(i int) bool { return p.Hosts[i].HostKey >= atHostKey })

	loHiHost := atHostKey - 1
	if atHostKey == 0 {
		loHiHost = 0
	}
	lo = &Partition{
		ID:           p.ID,
		HostKeyLo:    p.HostKeyLo,
		HostKeyHi:    loHiHost,
		CreatedHours: p.CreatedHours,
		DefaultCodec: p.DefaultCodec,
		URLs:         append([]m.URLRecord(nil), p.URLs[:ui]...),
		Hosts:        append([]m.HostRecord(nil), p.Hosts[:hi0]...),
		Meta:         cloneMeta(p.Meta),
	}
	hi = &Partition{
		ID:           p.ID,
		HostKeyLo:    atHostKey,
		HostKeyHi:    p.HostKeyHi,
		CreatedHours: p.CreatedHours,
		DefaultCodec: p.DefaultCodec,
		URLs:         append([]m.URLRecord(nil), p.URLs[ui:]...),
		Hosts:        append([]m.HostRecord(nil), p.Hosts[hi0:]...),
		Meta:         cloneMeta(p.Meta),
	}
	rebaseArena(lo, p.Strings)
	rebaseArena(hi, p.Strings)
	return lo, hi
}

// Merge combines two partitions with disjoint HostKey ranges into one, keeping
// both tables sorted and the string arena shared. It is the inverse of Split,
// the rebalance step that folds two small partitions back together. Merge
// returns ErrNotSorted if the merged URL rows would not be sorted, which catches
// overlapping or mis-ordered inputs rather than producing a file a reader would
// reject. The lower-keyed partition's identity and creation time win.
func Merge(a, b *Partition) (*Partition, error) {
	if b.HostKeyLo < a.HostKeyLo {
		a, b = b, a
	}
	urls := make([]m.URLRecord, 0, len(a.URLs)+len(b.URLs))
	urls = append(urls, a.URLs...)
	urls = append(urls, b.URLs...)
	hosts := make([]m.HostRecord, 0, len(a.Hosts)+len(b.Hosts))
	hosts = append(hosts, a.Hosts...)
	hosts = append(hosts, b.Hosts...)
	if !sortedURLs(urls) || !sortedHosts(hosts) {
		return nil, ErrNotSorted
	}

	out := &Partition{
		ID:           a.ID,
		HostKeyLo:    a.HostKeyLo,
		HostKeyHi:    maxU64(a.HostKeyHi, b.HostKeyHi),
		CreatedHours: a.CreatedHours,
		DefaultCodec: a.DefaultCodec,
		Meta:         cloneMeta(a.Meta),
	}
	// Re-base both source arenas into one. Each source has its own offset cache,
	// because an offset in a's arena and the same offset in b's name different
	// spans, so the caches must not be shared.
	arena := newArena()
	ca := newRebaser(a.Strings)
	cb := newRebaser(b.Strings)
	for i := range a.URLs {
		r := a.URLs[i]
		arena = ca.copyURL(&r, arena)
		out.URLs = append(out.URLs, r)
	}
	for i := range b.URLs {
		r := b.URLs[i]
		arena = cb.copyURL(&r, arena)
		out.URLs = append(out.URLs, r)
	}
	for i := range a.Hosts {
		r := a.Hosts[i]
		arena = ca.copyHost(&r, arena)
		out.Hosts = append(out.Hosts, r)
	}
	for i := range b.Hosts {
		r := b.Hosts[i]
		arena = cb.copyHost(&r, arena)
		out.Hosts = append(out.Hosts, r)
	}
	out.Strings = arena
	return out, nil
}

// rebaser copies spans from one source arena into a destination arena, caching
// by source offset so a repeated reference points at one copy, keeping the
// destination free of the duplicates a shared string would otherwise create.
type rebaser struct {
	src   []byte
	cache map[uint64]uint64
}

func newRebaser(src []byte) *rebaser {
	return &rebaser{src: src, cache: map[uint64]uint64{}}
}

// intern copies the span at off in the source to the end of arena and returns
// the grown arena and the span's new offset. A zero or out-of-range offset is
// the none sentinel and maps to 0.
func (rb *rebaser) intern(off uint64, arena []byte) ([]byte, uint64) {
	if off == 0 {
		return arena, 0
	}
	if v, ok := rb.cache[off]; ok {
		return arena, v
	}
	span := arenaRead(rb.src, off)
	if span == nil {
		return arena, 0
	}
	arena, newOff := arenaIntern(arena, span)
	rb.cache[off] = newOff
	return arena, newOff
}

// copyURL re-bases a URL row's string references into arena, returning the grown
// arena. A zero ref stays zero, the none sentinel.
func (rb *rebaser) copyURL(r *m.URLRecord, arena []byte) []byte {
	arena, r.URLRef = rb.intern(r.URLRef, arena)
	arena, r.ETagRef = rb.intern(r.ETagRef, arena)
	arena, r.RedirectRef = rb.intern(r.RedirectRef, arena)
	return arena
}

func (rb *rebaser) copyHost(r *m.HostRecord, arena []byte) []byte {
	arena, r.HostRef = rb.intern(r.HostRef, arena)
	arena, r.RegistrableRef = rb.intern(r.RegistrableRef, arena)
	arena, r.RobotsRef = rb.intern(r.RobotsRef, arena)
	return arena
}

// rebaseArena rebuilds dst.Strings from src to hold only the spans dst's rows
// reference, rewriting every *Ref in place.
func rebaseArena(dst *Partition, src []byte) {
	arena := newArena()
	rb := newRebaser(src)
	for i := range dst.URLs {
		arena = rb.copyURL(&dst.URLs[i], arena)
	}
	for i := range dst.Hosts {
		arena = rb.copyHost(&dst.Hosts[i], arena)
	}
	dst.Strings = arena
}

func cloneMeta(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
