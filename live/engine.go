package live

import (
	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/format"
)

// Engine is the file-backed live store of spec 2073 doc 08: the mapped .meguri file
// is the durable state, and the engine reads dedup, lookup, and schedule straight
// off it. The base is the mapped file (read-only, reclaimable page cache), the
// filter is the resident blocked Bloom that answers "is this new" without touching
// the file, and the file's sorted keys are the exact set a filter hit is confirmed
// against. There is no DRUM, no append log, and no spilled arena; the only resident
// per-URL cost is the filter.
//
// This is the read and dedup half. The write half (a bounded delta plus compaction)
// layers on top and is what turns a discovery into a new file generation.
type Engine struct {
	path     string
	file     []byte // the mmap'd base file bytes, not a copy
	closeMap func() error
	base     *format.Reader
	filter   *dedup.Filter
	hostLo   uint64
	hostHi   uint64
	urlCount int
}

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
	var filter *dedup.Filter
	if len(fb) > 0 {
		filter, err = dedup.LoadFilter(fb)
		if err != nil {
			_ = closer()
			return nil, err
		}
	}
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
	_, ok, err := e.base.LookupURL(key)
	return ok, err
}

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
