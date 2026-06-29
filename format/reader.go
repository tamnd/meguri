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
