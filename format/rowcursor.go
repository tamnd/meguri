package format

import m "github.com/tamnd/meguri"

// URLRowCursor streams the URL table of a .meguri file row by row in stored order
// (URLKey ascending), decoding one page of all columns at a time and yielding its
// rows before moving to the next page. It is the sequential read the checkpoint
// needs to source base-record bodies from the prior snapshot: a checkpoint that
// re-folds the live frontier reads every unchanged record straight off the file in
// key order, merge-joined against the log tail, instead of a per-key page decode or
// a random-read gather over the body log (doc 14, the body-ordered store). The
// transient is one page of records, not the table.
type URLRowCursor struct {
	r        *Reader
	dirByID  map[int]columnDir
	pageRecs []m.URLRecord
	ri       int  // next row within pageRecs
	pi       int  // next page index to decode
	npages   int  // page count (1 for a single-page table)
	single   bool // single-page table: one whole-column decode, no skip list
}

// URLRows opens a sequential row cursor over the file's URL table. A single-page
// table decodes its columns once on the first Next; a multi-page table decodes a
// page at a time, so the resident cost is one page regardless of the table size.
func (r *Reader) URLRows() (*URLRowCursor, error) {
	dirByID := make(map[int]columnDir, len(r.footer.urlDir))
	for _, d := range r.footer.urlDir {
		dirByID[d.columnID] = d
	}
	hkDir, ok := dirByID[colURLHostKey]
	if !ok {
		return nil, ErrCorrupt
	}
	c := &URLRowCursor{r: r, dirByID: dirByID}
	if hkDir.numPages <= 1 || len(hkDir.pages) == 0 {
		c.single = true
		c.npages = 1
	} else {
		c.npages = len(hkDir.pages)
	}
	return c, nil
}

// Next returns the next URL record in stored order. ok is false at the end of the
// table.
func (c *URLRowCursor) Next() (m.URLRecord, bool, error) {
	for c.ri >= len(c.pageRecs) {
		if c.pi >= c.npages {
			return m.URLRecord{}, false, nil
		}
		recs, err := c.decodePage(c.pi)
		if err != nil {
			return m.URLRecord{}, false, err
		}
		c.pi++
		c.pageRecs = recs
		c.ri = 0
	}
	rec := c.pageRecs[c.ri]
	c.ri++
	return rec, true, nil
}

// decodePage decodes every column of one page (or the whole single-page table) and
// assembles the rows. It is the per-page transient the cursor holds live.
func (c *URLRowCursor) decodePage(pi int) ([]m.URLRecord, error) {
	if c.single {
		cols, err := c.r.projectAllURL()
		if err != nil {
			return nil, err
		}
		return urlRecordsFromColumns(cols, c.r.URLCount())
	}
	nv := int(c.dirByID[colURLHostKey].pages[pi].numValues)
	cols := make(map[int][]byte, urlColumnCount)
	for id := range urlColumnCount {
		d, ok := c.dirByID[id]
		if !ok {
			return nil, ErrCorrupt
		}
		page, err := decodeColumnPage(c.r.file, d, pi)
		if err != nil {
			return nil, err
		}
		cols[id] = page
	}
	return urlRecordsFromColumns(cols, nv)
}
