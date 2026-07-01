package live

import (
	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/format"
)

// Engine is the file-backed live store of spec 2073 doc 08: the mapped .meguri file
// is the durable state, and the engine reads dedup, lookup, and schedule straight
// off it. The base is the mapped file (read-only, reclaimable page cache), the
// filter is the resident one-sided membership filter (the ribbon snapshot the seal
// writes, or the blocked Bloom an older file carries) that answers "is this new"
// without touching the file, and the file's sorted keys are the exact set a hit is confirmed
// against. There is no DRUM, no append log, and no spilled arena; the only resident
// per-URL cost is the filter.
//
// This is the read and dedup half. The write half (a bounded delta plus compaction)
// layers on top and is what turns a discovery into a new file generation.
type Engine struct {
	path       string
	file       []byte // the mmap'd base file bytes, not a copy
	closeMap   func() error
	base       *format.Reader
	filter     *dedup.ResidentFilter
	hostLo     uint64
	hostHi     uint64
	urlCount   int
	baseProbes uint64 // dedup decisions that fell through the filter to the base
}

// keyCachePages is the decoded key-page cache the engine installs on its reader so
// the slow path (a filter hit confirmed against the base) amortizes the page
// decode across a clustered probe stream. The base is host-clustered, so a recrawl
// reading a host's keys in order hits one page repeatedly; a small cache turns all
// but the first probe per page into a binary search over already-decoded bytes.
const keyCachePages = 64

// Open maps the .meguri file and loads the resident filter from its seen-set
// region. The file is mapped, not read, so a multi-gigabyte base costs reclaimable
// page cache and not heap (the goal-box property). If the file has no seen-set
// region the filter is left nil and dedup falls through to the file for every key,
// which is correct but slower; a file BulkLoad wrote always carries the region.
func Open(path string) (*Engine, error) {
	b, closer, err := mmapFile(path)
	if err != nil {
		return nil, err
	}
	adviseRandom(b)
	r, err := format.NewReader(b)
	if err != nil {
		_ = closer()
		return nil, err
	}
	fb, err := r.SeenFilter()
	if err != nil {
		_ = closer()
		return nil, err
	}
	var filter *dedup.ResidentFilter
	if len(fb) > 0 {
		filter, err = dedup.UnmarshalFilter(fb)
		if err != nil {
			_ = closer()
			return nil, err
		}
	}
	r.EnableKeyCache(keyCachePages)
	h := r.Header()
	return &Engine{
		path:     path,
		file:     b,
		closeMap: closer,
		base:     r,
		filter:   filter,
		hostLo:   h.HostKeyLo,
		hostHi:   h.HostKeyHi,
		urlCount: r.URLCount(),
	}, nil
}

// Seen reports whether key is already in the store, the dedup decision. A filter
// miss is an authoritative "new", returned without faulting a single file page,
// which is the common case on a discovery stream and why intake does not thrash the
// map. A filter hit is confirmed against the mapped base: a true positive is a
// rediscovery, a false positive decodes one page and finds nothing.
func (e *Engine) Seen(key m.URLKey) (bool, error) {
	if e.filter != nil && !e.filter.MaybeContains(key) {
		return false, nil
	}
	e.baseProbes++
	return e.base.ContainsURL(key)
}

// BaseProbes is the number of dedup decisions that fell through the resident
// filter to the mapped base, the slow-path count. On a fresh discovery stream it
// is the filter's false positives; on a recrawl it is the rediscoveries. It is the
// honest split between the resident fast path and the file-touching slow path, the
// metric the residency claim of doc 08 turns on.
func (e *Engine) BaseProbes() uint64 { return e.baseProbes }

// GetURL returns the record for key and whether it was found. It takes the same
// filter-then-base path as Seen, so an absent key that the filter rules out costs
// no file access.
func (e *Engine) GetURL(key m.URLKey) (m.URLRecord, bool, error) {
	if e.filter != nil && !e.filter.MaybeContains(key) {
		return m.URLRecord{}, false, nil
	}
	return e.base.LookupURL(key)
}

// Reader exposes the underlying file reader for the schedule and recrawl read
// paths (DueByWheel, Schedule, URLRows), which read the file directly.
func (e *Engine) Reader() *format.Reader { return e.base }

// DueCursor opens a bounded due-dispatch scan over the base file for the rows due
// at or before now, the scheduler read of doc 08 Stage 3. It streams due keys off
// the mapped file in caller-capped batches rather than materializing the whole
// next_due column, so a scheduler pulls the next work at 100M without a
// multi-gigabyte transient. A now the file's footer proves nothing is due for
// yields an empty cursor without decoding a column.
func (e *Engine) DueCursor(now uint32) (*format.DueCursor, error) {
	return e.base.DueCursor(now)
}

// URLCount is the number of URLs in the base file.
func (e *Engine) URLCount() int { return e.urlCount }

// HostKeyRange is the partition's host-key span, the redistribution and routing
// key range.
func (e *Engine) HostKeyRange() (lo, hi uint64) { return e.hostLo, e.hostHi }

// BitsPerURL reports the resident filter cost per URL, the residency budget term.
func (e *Engine) BitsPerURL() float64 {
	if e.filter == nil {
		return 0
	}
	return e.filter.BitsPerURL()
}

// Close unmaps the base file.
func (e *Engine) Close() error { return e.closeMap() }
