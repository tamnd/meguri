package format

import m "github.com/tamnd/meguri"

// Reader is a projection-and-pushdown view over a .meguri file. Where Decode
// materializes every column of every row into full records, a Reader parses the
// header and footer once and then decodes only the columns a caller asks for,
// and lets a caller skip the file entirely when the footer's zone maps prove no
// row can match a predicate (doc 10 section 9, predicate pushdown and projection
// discipline). This is what keeps a scheduler scan, a dedup rebuild, and a host
// range read from paying for the 21 columns they do not read.
//
// A Reader holds the file bytes; it does not copy them. The decoded columns it
// returns are fresh slices.
type Reader struct {
	file   []byte
	header *Header
	footer *footerData
	keyc   *keyCache
}

// keyPage is one decoded key-column page: the hostkey and pathkey bytes and the
// row count they hold. It is what a presence check binary-searches, cached so a
// clustered probe stream (a recrawl reading many keys in the same host) does not
// re-decompress the same page on every probe.
type keyPage struct {
	hk, pk []byte
	n      int
}

// keyCache is a small fixed-capacity cache of decoded key pages with ring
// eviction. It turns the base presence check from a page decode per probe into a
// decode per distinct hot page, the amortization the file-backed engine's slow
// path (a filter hit confirmed against the base) needs so a rediscovery-heavy or
// recrawl stream does not pay a full zstd+FSST page decode on every key. It is not
// safe for concurrent use, matching the single-goroutine read path it serves.
type keyCache struct {
	cap   int
	pages map[int]keyPage
	ring  []int
	next  int
}

// EnableKeyCache installs a decoded key-page cache of capPages pages on the
// reader, so repeated presence checks into the same pages amortize the decode.
// capPages <= 0 disables it. It is opt-in because a one-shot scan gains nothing
// from it and a cache holds decoded pages resident; the live engine turns it on
// for the probe-stream read path.
func (r *Reader) EnableKeyCache(capPages int) {
	if capPages <= 0 {
		r.keyc = nil
		return
	}
	r.keyc = &keyCache{
		cap:   capPages,
		pages: make(map[int]keyPage, capPages),
		ring:  make([]int, 0, capPages),
	}
}

// keyPageAt returns the decoded key columns of page pi, serving them from the
// cache when present and decoding and caching them otherwise. With no cache
// installed it decodes every time, the unchanged behavior.
func (r *Reader) keyPageAt(hkDir, pkDir columnDir, pi, nv int) (keyPage, error) {
	if r.keyc != nil {
		if kp, ok := r.keyc.pages[pi]; ok {
			return kp, nil
		}
	}
	hk, err := decodeColumnPage(r.file, hkDir, pi)
	if err != nil {
		return keyPage{}, err
	}
	pk, err := decodeColumnPage(r.file, pkDir, pi)
	if err != nil {
		return keyPage{}, err
	}
	kp := keyPage{hk: hk, pk: pk, n: nv}
	if r.keyc != nil {
		c := r.keyc
		if len(c.ring) < c.cap {
			c.ring = append(c.ring, pi)
		} else {
			delete(c.pages, c.ring[c.next])
			c.ring[c.next] = pi
			c.next = (c.next + 1) % c.cap
		}
		c.pages[pi] = kp
	}
	return kp, nil
}

// NewReader parses a .meguri file's header and footer, verifying their
// checksums, without decoding any column body. It is the cheap open a pushdown
// query starts from: the zone maps and stats it needs to decide whether to read
// further all live in the footer.
func NewReader(b []byte) (*Reader, error) {
	if len(b) < HeaderSize+trailerSize {
		return nil, ErrShortFile
	}
	h, err := DecodeHeader(b[:HeaderSize])
	if err != nil {
		return nil, err
	}
	if [4]byte(b[len(b)-4:]) != Magic {
		return nil, ErrBadMagic
	}
	r := &rbuf{b: b[len(b)-trailerSize:]}
	footerLen := int(r.u32())
	footerCRC := r.u32()
	footerStart := len(b) - trailerSize - footerLen
	if footerStart < HeaderSize || footerStart != int(h.FooterOffset) {
		return nil, ErrCorrupt
	}
	footerBytes := b[footerStart : len(b)-trailerSize]
	if crc32c(footerBytes) != footerCRC {
		return nil, ErrChecksum
	}
	f, err := decodeFooter(footerBytes)
	if err != nil {
		return nil, err
	}
	return &Reader{file: b, header: h, footer: f}, nil
}

// URLCount and HostCount report the row counts from the header.
func (r *Reader) URLCount() int  { return int(r.header.URLCount) }
func (r *Reader) HostCount() int { return int(r.header.HostCount) }

// HostKeyRange returns the partition's HostKey bounds from the header, the range
// a router uses to decide whether this file could own a host at all.
func (r *Reader) HostKeyRange() (lo, hi uint64) {
	return r.header.HostKeyLo, r.header.HostKeyHi
}

// MaybeOwnsHost reports whether the host could live in this partition, a O(1)
// pushdown from the header's HostKey range: a false is authoritative (the host
// is outside the partition's range), a true means the file must be read to
// confirm. This is the host range read of doc 10 section 9.
func (r *Reader) MaybeOwnsHost(hostKey uint64) bool {
	return hostKey >= r.header.HostKeyLo && hostKey <= r.header.HostKeyHi
}

// DueRange returns the smallest and largest nonzero next_due across the URL
// rows, as the footer stats recorded them. A dueMin of 0 means no row is
// scheduled.
func (r *Reader) DueRange() (min, max uint32) {
	return r.footer.stats.dueMin, r.footer.stats.dueMax
}

// MaybeDueAt reports whether any URL could be due at or before now, the
// file-level pushdown a scheduler uses to skip a partition with no due work
// without decoding a single column: false means the soonest due time is in the
// future (or nothing is scheduled), true means the next_due column must be read.
func (r *Reader) MaybeDueAt(now uint32) bool {
	min := r.footer.stats.dueMin
	return min != 0 && min <= now
}

// URLZone returns a URL column's zone min/max from the footer directory, the
// per-column pushdown bound. ok is false for a column with no zone map (a float
// or opaque-byte column). The column id is one of the ColURL* constants.
func (r *Reader) URLZone(col int) (min, max uint64, ok bool) {
	for _, d := range r.footer.urlDir {
		if d.columnID == col {
			return d.zoneMin, d.zoneMax, d.hasZone
		}
	}
	return 0, 0, false
}

// Header returns the file's decoded header: partition id, build time, codec, key
// range, and row counts. It is the cheap metadata a recovering store reads to
// rebuild its shell without decoding a single body column.
func (r *Reader) Header() Header { return *r.header }

// SeenFilter returns the serialized resident seen-set filter from the file's
// seen-set region, or nil if the file has none. It is the bytes dedup.LoadFilter
// restores, so a recovering engine reloads the approximate dedup tier from the
// mapped file without re-adding every key (spec 2073 doc 08, the recovery path).
func (r *Reader) SeenFilter() ([]byte, error) {
	reg, ok := findRegion(r.footer.regions, RegionSeenset)
	if !ok {
		return nil, nil
	}
	return decodeSeensetRegion(r.file[reg.offset : reg.offset+reg.length])
}

// Hosts decodes the whole host table into records. The host table is the small
// region of the file, one row per host rather than per URL, so it is materialized
// whole where the URL table streams. A recovering engine reads it to rebuild the
// resident host map from the mapped file without decoding the URL body.
func (r *Reader) Hosts() ([]m.HostRecord, error) {
	cols, err := decodeColumnRegion(r.file, r.footer.hostDir)
	if err != nil {
		return nil, err
	}
	return hostRecordsFromColumns(cols, r.HostCount())
}

// projectURL decodes only the named URL columns, verifying each one's CRC. It is
// the projection primitive: a caller that needs three columns pays to decode
// three, not all twenty-three. An unknown column id is skipped, so the returned
// map carries exactly the ids that existed.
func (r *Reader) projectURL(cols ...int) (map[int][]byte, error) {
	want := make(map[int]bool, len(cols))
	for _, c := range cols {
		want[c] = true
	}
	dir := make([]columnDir, 0, len(cols))
	for _, d := range r.footer.urlDir {
		if want[d.columnID] {
			dir = append(dir, d)
		}
	}
	return decodeColumnRegion(r.file, dir)
}

// URLKeys decodes only the two key columns and reconstructs the partition's
// URLKeys, the urlkey-only dedup projection of doc 10 section 9: a seen-set
// rebuild reads the keys without paying for status, timestamps, or fingerprints.
func (r *Reader) URLKeys() ([]m.URLKey, error) {
	cols, err := r.projectURL(colURLHostKey, colURLPathKey)
	if err != nil {
		return nil, err
	}
	n := r.URLCount()
	hk, pk := cols[colURLHostKey], cols[colURLPathKey]
	if len(hk) < n*8 || len(pk) < n*8 {
		return nil, ErrCorrupt
	}
	out := make([]m.URLKey, n)
	for i := range out {
		out[i] = m.URLKey{HostKey: getU64(hk, i), PathKey: getU64(pk, i)}
	}
	return out, nil
}

// NextDue decodes only the next_due column, the next_due-only scan of doc 10
// section 9: a scheduler reads the due times without materializing records.
func (r *Reader) NextDue() ([]uint32, error) {
	cols, err := r.projectURL(colURLNextDue)
	if err != nil {
		return nil, err
	}
	n := r.URLCount()
	nd := cols[colURLNextDue]
	if len(nd) < n*4 {
		return nil, ErrCorrupt
	}
	out := make([]uint32, n)
	for i := range out {
		out[i] = getU32(nd, i)
	}
	return out, nil
}

// DueKeys returns the URLKeys of every row due at or before now, combining the
// file-level pushdown with a two-column projection: when the footer proves
// nothing is due it returns nil without decoding a body, otherwise it reads only
// the urlkey and next_due columns. A due row has a nonzero next_due at or before
// now (a zero next_due is unscheduled). This is the scheduler's read path made
// cheap, the worked example of doc 10 section 9.
func (r *Reader) DueKeys(now uint32) ([]m.URLKey, error) {
	if !r.MaybeDueAt(now) {
		return nil, nil
	}
	cols, err := r.projectURL(colURLHostKey, colURLPathKey, colURLNextDue)
	if err != nil {
		return nil, err
	}
	n := r.URLCount()
	hk, pk, nd := cols[colURLHostKey], cols[colURLPathKey], cols[colURLNextDue]
	if len(hk) < n*8 || len(pk) < n*8 || len(nd) < n*4 {
		return nil, ErrCorrupt
	}
	var out []m.URLKey
	for i := range n {
		due := getU32(nd, i)
		if due != 0 && due <= now {
			out = append(out, m.URLKey{HostKey: getU64(hk, i), PathKey: getU64(pk, i)})
		}
	}
	return out, nil
}

// HostRangeURLKeys returns the URLKeys of every row whose host key falls in
// [lo, hi], the host range read of doc 10 section 9 sharpened to the page level.
// When the hostkey column is split across pages (Partition.MaxPageRows opted in),
// it consults the per-page skip list and decodes only the pages whose zone
// overlaps [lo, hi], pruning the rest without decompressing them; the pathkey
// column splits on the same boundaries, so the same page indices line up. A
// single-page column falls back to a whole-column projection and filter, and a
// range disjoint from the partition's header bounds returns nil without touching
// the body. The rows stay in stored order, which is host-key ascending.
func (r *Reader) HostRangeURLKeys(lo, hi uint64) ([]m.URLKey, error) {
	if lo > hi || hi < r.header.HostKeyLo || lo > r.header.HostKeyHi {
		return nil, nil
	}
	var hkDir, pkDir columnDir
	var okHK, okPK bool
	for _, d := range r.footer.urlDir {
		switch d.columnID {
		case colURLHostKey:
			hkDir, okHK = d, true
		case colURLPathKey:
			pkDir, okPK = d, true
		}
	}
	if !okHK || !okPK {
		return nil, ErrCorrupt
	}

	// Single-page column: no skip list to prune with, so read the keys whole and
	// filter. This is the unchanged behavior for a partition that did not opt into
	// page splitting.
	if hkDir.numPages <= 1 || len(hkDir.pages) == 0 {
		keys, err := r.URLKeys()
		if err != nil {
			return nil, err
		}
		var out []m.URLKey
		for _, k := range keys {
			if k.HostKey >= lo && k.HostKey <= hi {
				out = append(out, k)
			}
		}
		return out, nil
	}

	var out []m.URLKey
	for pi := range hkDir.pages {
		pe := hkDir.pages[pi]
		if pe.hasZone && (pe.zoneMax < lo || pe.zoneMin > hi) {
			continue // this page's host keys are entirely outside the range
		}
		hk, err := decodeColumnPage(r.file, hkDir, pi)
		if err != nil {
			return nil, err
		}
		pk, err := decodeColumnPage(r.file, pkDir, pi)
		if err != nil {
			return nil, err
		}
		nv := int(pe.numValues)
		if len(hk) < nv*8 || len(pk) < nv*8 {
			return nil, ErrCorrupt
		}
		for i := range nv {
			v := getU64(hk, i)
			if v >= lo && v <= hi {
				out = append(out, m.URLKey{HostKey: v, PathKey: getU64(pk, i)})
			}
		}
	}
	return out, nil
}

// HostRangePageScan reports how many of the hostkey column's pages a host-range
// read for [lo, hi] would decode versus the column's total page count, the
// observable proof that the per-page skip list pruned pages. total is 0 for a
// single-page column, which carries no skip list to prune.
func (r *Reader) HostRangePageScan(lo, hi uint64) (scanned, total int) {
	for _, d := range r.footer.urlDir {
		if d.columnID != colURLHostKey {
			continue
		}
		total = len(d.pages)
		for _, pe := range d.pages {
			if !pe.hasZone || (pe.zoneMax >= lo && pe.zoneMin <= hi) {
				scanned++
			}
		}
		return scanned, total
	}
	return 0, 0
}

// HasSchedule reports whether the file carries a schedule index region, the
// durable timing wheel a scheduler can read instead of scanning the next_due
// column.
func (r *Reader) HasSchedule() bool {
	_, ok := findRegion(r.footer.regions, RegionSchedule)
	return ok
}

// Schedule decodes the schedule index region into a wheel a scheduler queries
// with DueBuckets, or returns nil when the file carries no schedule region. It
// verifies the page CRC. This is the pushdown read for due work: the wheel
// narrows the scan to the buckets whose window has opened, the next_due column
// confirms the survivors.
func (r *Reader) Schedule() (*ScheduleIndex, error) {
	reg, ok := findRegion(r.footer.regions, RegionSchedule)
	if !ok {
		return nil, nil
	}
	return decodeScheduleRegion(r.file[reg.offset : reg.offset+reg.length])
}

// DueByWheel returns the URLKeys of every row due at or before now, using the
// schedule wheel to prune the scan: it reads the wheel's due buckets, then
// projects the urlkey and next_due columns only for those candidate rows and
// confirms each against next_due. It falls back to the column-scan DueKeys when
// the file carries no wheel. This is the schedule-index read path of doc 10
// section 7.
func (r *Reader) DueByWheel(now uint32) ([]m.URLKey, error) {
	idx, err := r.Schedule()
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return r.DueKeys(now)
	}
	cand := idx.DueBuckets(now)
	if len(cand) == 0 {
		return nil, nil
	}
	cols, err := r.projectURL(colURLHostKey, colURLPathKey, colURLNextDue)
	if err != nil {
		return nil, err
	}
	n := r.URLCount()
	hk, pk, nd := cols[colURLHostKey], cols[colURLPathKey], cols[colURLNextDue]
	if len(hk) < n*8 || len(pk) < n*8 || len(nd) < n*4 {
		return nil, ErrCorrupt
	}
	var out []m.URLKey
	for _, i := range cand {
		ri := int(i)
		if ri < 0 || ri >= n {
			return nil, ErrCorrupt
		}
		due := getU32(nd, ri)
		if due != 0 && due <= now {
			out = append(out, m.URLKey{HostKey: getU64(hk, ri), PathKey: getU64(pk, ri)})
		}
	}
	return out, nil
}

// LookupURL returns the URL record stored under key, or ok=false when the
// partition does not hold it. It is the point read the live store needs once the
// .meguri file is the body store rather than the append log (doc 03 change 3): a
// GetURL that misses the in-memory tail resolves the base record here by key,
// with no log byte offset to chase. The lookup is a zone-pruned page search. The
// header HostKey range rejects an out-of-partition key in O(1). For a multi-page
// table the hostkey column's per-page skip list narrows the scan to the pages
// whose zone covers the key's host, and a binary search inside each candidate
// page finds the row by full URLKey, the rows being stored in URLKey order (the
// streamed repository's order). A single-page table decodes its columns once and
// binary-searches the whole set.
func (r *Reader) LookupURL(key m.URLKey) (m.URLRecord, bool, error) {
	var zero m.URLRecord
	if key.HostKey < r.header.HostKeyLo || key.HostKey > r.header.HostKeyHi {
		return zero, false, nil
	}

	dirByID := make(map[int]columnDir, len(r.footer.urlDir))
	for _, d := range r.footer.urlDir {
		dirByID[d.columnID] = d
	}
	hkDir, ok := dirByID[colURLHostKey]
	if !ok {
		return zero, false, ErrCorrupt
	}

	// Single-page table: no skip list to prune with, so decode the columns whole
	// once and binary-search the full set. This is the unchanged shape for a
	// partition that did not opt into page splitting.
	if hkDir.numPages <= 1 || len(hkDir.pages) == 0 {
		cols, err := r.projectAllURL()
		if err != nil {
			return zero, false, err
		}
		recs, err := urlRecordsFromColumns(cols, r.URLCount())
		if err != nil {
			return zero, false, err
		}
		if rec, found := searchURLRecords(recs, key); found {
			return rec, true, nil
		}
		return zero, false, nil
	}

	// Multi-page: decode only the pages whose hostkey zone covers key.HostKey. A
	// host's rows can span more than one page, so every candidate is searched, but
	// pages whose [zoneMin, zoneMax] excludes the host are skipped without a decode.
	for pi := range hkDir.pages {
		pe := hkDir.pages[pi]
		if pe.hasZone && (pe.zoneMax < key.HostKey || pe.zoneMin > key.HostKey) {
			continue
		}
		cols := make(map[int][]byte, urlColumnCount)
		for id := range urlColumnCount {
			d, ok := dirByID[id]
			if !ok {
				return zero, false, ErrCorrupt
			}
			page, err := decodeColumnPage(r.file, d, pi)
			if err != nil {
				return zero, false, err
			}
			cols[id] = page
		}
		recs, err := urlRecordsFromColumns(cols, int(pe.numValues))
		if err != nil {
			return zero, false, err
		}
		if rec, found := searchURLRecords(recs, key); found {
			return rec, true, nil
		}
	}
	return zero, false, nil
}

// ContainsURL reports whether key is stored in the partition, the dedup-confirm
// primitive the file-backed engine runs when its resident filter says "maybe"
// (spec 2073 doc 08). It is LookupURL stripped to a presence test: it decodes only
// the two key columns, not the full thirteen-column record, so a confirmation
// costs a key-column page decode rather than a whole-row projection. The header
// host range rejects an out-of-partition key in O(1); a multi-page table prunes to
// the pages whose hostkey zone covers the key and binary-searches each by URLKey;
// a single-page table decodes its key columns once and binary-searches the set.
func (r *Reader) ContainsURL(key m.URLKey) (bool, error) {
	if key.HostKey < r.header.HostKeyLo || key.HostKey > r.header.HostKeyHi {
		return false, nil
	}
	var hkDir, pkDir columnDir
	var okHK, okPK bool
	for _, d := range r.footer.urlDir {
		switch d.columnID {
		case colURLHostKey:
			hkDir, okHK = d, true
		case colURLPathKey:
			pkDir, okPK = d, true
		}
	}
	if !okHK || !okPK {
		return false, ErrCorrupt
	}

	// Single-page table: decode the two key columns whole once and binary-search.
	if hkDir.numPages <= 1 || len(hkDir.pages) == 0 {
		keys, err := r.URLKeys()
		if err != nil {
			return false, err
		}
		return searchURLKeys(keys, key), nil
	}

	// Multi-page: decode the key columns only for pages whose hostkey zone covers
	// the key, binary-searching each candidate. A host's rows can span pages, so
	// every zone-covering page is searched, but the rest are skipped without a
	// decode.
	for pi := range hkDir.pages {
		pe := hkDir.pages[pi]
		if pe.hasZone && (pe.zoneMax < key.HostKey || pe.zoneMin > key.HostKey) {
			continue
		}
		nv := int(pe.numValues)
		kp, err := r.keyPageAt(hkDir, pkDir, pi, nv)
		if err != nil {
			return false, err
		}
		if len(kp.hk) < nv*8 || len(kp.pk) < nv*8 {
			return false, ErrCorrupt
		}
		if searchKeyColumns(kp.hk, kp.pk, nv, key) {
			return true, nil
		}
	}
	return false, nil
}

// searchURLKeys binary-searches a URLKey-ordered slice for key.
func searchURLKeys(keys []m.URLKey, key m.URLKey) bool {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch keys[mid].Compare(key) {
		case 0:
			return true
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

// searchKeyColumns binary-searches the parallel hostkey and pathkey column bytes
// of one decoded page for key, the rows being stored URLKey-ascending.
func searchKeyColumns(hk, pk []byte, n int, key m.URLKey) bool {
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		cur := m.URLKey{HostKey: getU64(hk, mid), PathKey: getU64(pk, mid)}
		switch cur.Compare(key) {
		case 0:
			return true
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

// projectAllURL decodes every URL column, the full-record projection a point
// lookup needs to reconstruct a row. It is projectURL over the whole schema.
func (r *Reader) projectAllURL() (map[int][]byte, error) {
	ids := make([]int, urlColumnCount)
	for i := range ids {
		ids[i] = i
	}
	return r.projectURL(ids...)
}

// searchURLRecords binary-searches a URLKey-ordered record slice for key, the
// per-page probe LookupURL runs once the candidate page is decoded.
func searchURLRecords(recs []m.URLRecord, key m.URLKey) (m.URLRecord, bool) {
	lo, hi := 0, len(recs)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch recs[mid].URLKey.Compare(key) {
		case 0:
			return recs[mid], true
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return m.URLRecord{}, false
}

// Exported URL column ids, the stable schema positions a projection names. They
// match the on-disk column directory and never change once shipped (doc 10
// section 4, the URL table columns).
const (
	ColURLHostKey     = colURLHostKey
	ColURLPathKey     = colURLPathKey
	ColURLStatus      = colURLStatus
	ColURLNextDue     = colURLNextDue
	ColURLHTTPStatus  = colURLHTTPStatus
	ColURLCrawlCount  = colURLCrawlCount
	ColURLLastCrawled = colURLLastCrawled
)
