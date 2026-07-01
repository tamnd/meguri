package format

import (
	m "github.com/tamnd/meguri"
)

// DueCursor streams the URL rows due at or before a wall-clock hour off the file
// in bounded batches, so a scheduler pulls the next work without decoding the
// whole next_due column or materializing every due key at once. It walks the
// next_due, hostkey, and pathkey columns page by page, uses each next_due page's
// zone map to skip a page whose soonest due time is still in the future, and emits
// due keys up to a caller batch cap, remembering its position across calls. The
// transient is a few decoded pages plus the batch, not the column: at 100M, where
// DueKeys' whole-column projection plus its every-due-key slice would hold
// gigabytes, this is the scale-appropriate scheduler read.
type DueCursor struct {
	r      *Reader
	hkDir  columnDir
	pkDir  columnDir
	ndDir  columnDir
	now    uint32
	single bool // single-page columns: one synthetic page over the whole column

	pi    int    // next page index to consider
	row   int    // next row within the decoded page
	nv    int    // values in the decoded page
	hk    []byte // decoded hostkey page (or whole column when single)
	pk    []byte // decoded pathkey page
	nd    []byte // decoded next_due page
	haveP bool   // a page is decoded and not yet exhausted
}

// DueCursor opens a bounded due-dispatch scan for the rows due at or before now.
// When the file's footer proves nothing is due it returns a cursor that yields no
// batches, the same file-level pushdown DueKeys uses, so a partition with no due
// work costs no column decode. The next_due, hostkey, and pathkey columns must
// split on the same page boundaries (they always do, since the encoder pages the
// URL table uniformly), so a page index lines up across the three.
func (r *Reader) DueCursor(now uint32) (*DueCursor, error) {
	if !r.MaybeDueAt(now) {
		return &DueCursor{}, nil
	}
	var hkDir, pkDir, ndDir columnDir
	var okHK, okPK, okND bool
	for _, d := range r.footer.urlDir {
		switch d.columnID {
		case colURLHostKey:
			hkDir, okHK = d, true
		case colURLPathKey:
			pkDir, okPK = d, true
		case colURLNextDue:
			ndDir, okND = d, true
		}
	}
	if !okHK || !okPK || !okND {
		return nil, ErrCorrupt
	}
	c := &DueCursor{r: r, hkDir: hkDir, pkDir: pkDir, ndDir: ndDir, now: now}
	// A column that did not opt into page splitting carries no skip list, so there
	// is one synthetic page over the whole column and no per-page zone to prune with.
	c.single = ndDir.numPages <= 1 || len(ndDir.pages) == 0
	return c, nil
}

// loadPage decodes the three columns for page pi, or the whole column when the
// cursor is in single-page mode. It returns ok=false with no error when a
// multi-page candidate is pruned by its zone (its soonest due time is in the
// future), so the caller advances without holding the page.
func (c *DueCursor) loadPage(pi int) (ok bool, err error) {
	if c.single {
		c.hk, err = c.r.projectColumn(colURLHostKey)
		if err != nil {
			return false, err
		}
		c.pk, err = c.r.projectColumn(colURLPathKey)
		if err != nil {
			return false, err
		}
		c.nd, err = c.r.projectColumn(colURLNextDue)
		if err != nil {
			return false, err
		}
		c.nv = c.r.URLCount()
		if len(c.hk) < c.nv*8 || len(c.pk) < c.nv*8 || len(c.nd) < c.nv*4 {
			return false, ErrCorrupt
		}
		return true, nil
	}
	pe := c.ndDir.pages[pi]
	// The page's smallest next_due is in the future, so no row on it is due yet.
	// A zoneMin of 0 means the page holds unscheduled rows (next_due 0), which are
	// never due, so the page is only skippable when its whole zone sits past now.
	if pe.hasZone && pe.zoneMin > uint64(c.now) {
		return false, nil
	}
	c.nd, err = decodeColumnPage(c.r.file, c.ndDir, pi)
	if err != nil {
		return false, err
	}
	c.hk, err = decodeColumnPage(c.r.file, c.hkDir, pi)
	if err != nil {
		return false, err
	}
	c.pk, err = decodeColumnPage(c.r.file, c.pkDir, pi)
	if err != nil {
		return false, err
	}
	c.nv = int(pe.numValues)
	if len(c.nd) < c.nv*4 || len(c.hk) < c.nv*8 || len(c.pk) < c.nv*8 {
		return false, ErrCorrupt
	}
	return true, nil
}

// numPages is the page count the cursor walks: the real page count for a split
// column, or one synthetic page for a single-page column.
func (c *DueCursor) numPages() int {
	if c.single {
		return 1
	}
	return len(c.ndDir.pages)
}

// NextBatch returns up to limit URLKeys due at or before the cursor's now, in
// stored (host-key ascending) order, decoding only as many pages as it takes to
// fill the batch. It returns nil when the scan is exhausted, so a scheduler loops
// on NextBatch until it gets nil or has dispatched enough. A due row has a nonzero
// next_due at or before now; a zero next_due is unscheduled and never returned.
func (c *DueCursor) NextBatch(limit int) ([]m.URLKey, error) {
	if c.r == nil || limit <= 0 {
		return nil, nil
	}
	out := make([]m.URLKey, 0, limit)
	for len(out) < limit {
		if !c.haveP {
			if c.pi >= c.numPages() {
				break
			}
			pi := c.pi
			c.pi++
			ok, err := c.loadPage(pi)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue // pruned page, no due rows
			}
			c.row = 0
			c.haveP = true
		}
		for c.row < c.nv && len(out) < limit {
			i := c.row
			c.row++
			due := getU32(c.nd, i)
			if due != 0 && due <= c.now {
				out = append(out, m.URLKey{HostKey: getU64(c.hk, i), PathKey: getU64(c.pk, i)})
			}
		}
		if c.row >= c.nv {
			c.haveP = false
			c.hk, c.pk, c.nd = nil, nil, nil
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// projectColumn decodes one URL column whole, the single-page fallback the due
// cursor uses when the column carries no page skip list. It is the projectURL
// primitive narrowed to one column.
func (r *Reader) projectColumn(col int) ([]byte, error) {
	cols, err := r.projectURL(col)
	if err != nil {
		return nil, err
	}
	return cols[col], nil
}
