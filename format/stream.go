package format

import (
	"bufio"
	"hash/crc32"
	"io"
	"os"

	m "github.com/tamnd/meguri"
)

// meterSource wraps a URLRecordSource to observe the records as they stream past:
// the row count, the dueRange (min over nonzero NextDue, max over all, exactly as
// dueRange computes it for the footer), and whether the keys arrive in ascending
// order. The footer needs the count and the due range, and a checkpoint must never
// write an unsorted snapshot, so the meter recovers all three in the single pass
// without a second scan over the records.
type meterSource struct {
	inner          URLRecordSource
	n              int
	dueMin, dueMax uint32
	last           m.URLKey
	have           bool
	disordered     bool
}

func (s *meterSource) Next() (m.URLRecord, bool) {
	r, ok := s.inner.Next()
	if !ok {
		return r, false
	}
	s.n++
	if d := r.NextDue; d != 0 {
		if s.dueMin == 0 || d < s.dueMin {
			s.dueMin = d
		}
		if d > s.dueMax {
			s.dueMax = d
		}
	}
	if s.have && r.URLKey.Less(s.last) {
		s.disordered = true
	}
	s.last = r.URLKey
	s.have = true
	return r, true
}

// StreamEncodeToFile writes a complete .meguri file to path, streaming the URL
// table from src one record at a time instead of taking a materialized p.URLs. It
// is the bounded-memory checkpoint encoder (spec 2072 D9, 2071 implementation doc
// 51): the largest region, the URL table, never exists as a record slice or a
// column-major copy in memory; it is built page by page by StreamURLRegion and
// spilled to temp files. The host table, string blob, and optional seen-set are
// small and stay materialized in p, as in EncodeToFile. The output is a valid,
// byte-stable .meguri file identical to EncodeToFile fed the same records with the
// same MaxPageRows; the schedule region is not supported here because a checkpoint
// snapshot never builds one (p.BuildSchedule must be false).
//
// src must yield records in ascending URLKey order (the store's k-way shard merge
// does); the meter verifies it and the call fails with ErrNotSorted otherwise.
// maxRows should be > 0 so the per-column page buffers stay bounded; a zero keeps
// the single-page layout and reintroduces the O(rows) buffer the streaming exists
// to avoid.
func StreamEncodeToFile(path string, src URLRecordSource, maxRows int, p *Partition, tmpDir string) (err error) {
	if p.BuildSchedule {
		return ErrUnsupported
	}
	if !sortedHosts(p.Hosts) {
		return ErrNotSorted
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	w := bufio.NewWriterSize(f, 1<<20)

	codec := p.DefaultCodec
	pos := uint64(HeaderSize)
	if _, err = w.Write(make([]byte, HeaderSize)); err != nil {
		return err
	}

	var regions []regionDesc
	writeRegion := func(id uint8, region []byte) error {
		off := pos
		if _, e := w.Write(region); e != nil {
			return e
		}
		pos += uint64(len(region))
		regions = append(regions, regionDesc{id: id, offset: off, length: uint64(len(region)), crc: crc32c(region)})
		return nil
	}

	// URL table: streamed. StreamURLRegion writes the region bytes straight to w and
	// returns the directory, the byte length, and the region CRC, so the descriptor
	// matches the materialized path's writeRegion exactly.
	meter := &meterSource{inner: src}
	urlDir, urlLen, urlCRC, err := StreamURLRegion(w, meter, pos, maxRows, codec, tmpDir)
	if err != nil {
		return err
	}
	if meter.disordered {
		return ErrNotSorted
	}
	regions = append(regions, regionDesc{id: RegionURLTable, offset: pos, length: urlLen, crc: urlCRC})
	pos += urlLen
	urlCount := meter.n

	hostRegion, hostDir := encodeColumnRegion(hostColumns(p.Hosts, codec), pos, p.MaxPageRows)
	if err = writeRegion(RegionHostTable, hostRegion); err != nil {
		return err
	}
	hostRegion = nil

	if seen := encodeSeensetRegion(p.SeenFilter, uint64(urlCount), codec); len(seen) > 0 {
		if err = writeRegion(RegionSeenset, seen); err != nil {
			return err
		}
	}
	if p.StringsAt != nil {
		// Streaming blob region: the arena is read from p.StringsAt page by page so
		// the whole multi-gigabyte arena never lands in RAM. The region's CRC is
		// folded as the pages are written, matching writeRegion's descriptor.
		if p.StringsSize > 0 {
			off := pos
			rc := crc32.New(castagnoli)
			n, e := streamBlobRegion(io.MultiWriter(w, rc), p.StringsAt, p.StringsSize, 0, codec, p.BlobFrontCode)
			if e != nil {
				return e
			}
			pos += uint64(n)
			regions = append(regions, regionDesc{id: RegionStringBlob, offset: off, length: uint64(n), crc: rc.Sum32()})
		}
	} else if blob := encodeBlobRegion(p.Strings, codec, p.BlobFrontCode); len(blob) > 0 {
		if err = writeRegion(RegionStringBlob, blob); err != nil {
			return err
		}
	}

	footerOff := pos

	totalComp := dirTotals(urlDir) + dirTotals(hostDir)
	totalUncomp := uncompTotals(urlDir) + uncompTotals(hostDir)
	stats := statsBlock{
		urlCount:          uint64(urlCount),
		hostCount:         uint64(len(p.Hosts)),
		hostKeyLo:         p.HostKeyLo,
		hostKeyHi:         p.HostKeyHi,
		dueMin:            meter.dueMin,
		dueMax:            meter.dueMax,
		totalCompressed:   totalComp,
		totalUncompressed: totalUncomp,
	}
	if urlCount > 0 {
		stats.bytesPerURL = float32(footerOff) / float32(urlCount)
	}
	footerBytes := encodeFooter(&footerData{
		regions: regions,
		urlDir:  urlDir,
		hostDir: hostDir,
		stats:   stats,
		meta:    metaPairs(p.Meta),
	})
	if _, err = w.Write(footerBytes); err != nil {
		return err
	}

	var tail wbuf
	tail.u32(uint32(len(footerBytes)))
	tail.u32(crc32c(footerBytes))
	tail.bytes(Magic[:])
	if _, err = w.Write(tail.b); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}

	flags := FlagSorted
	if _, ok := findRegion(regions, RegionSeenset); ok {
		flags |= FlagHasSeenset
	}
	if _, ok := findRegion(regions, RegionStringBlob); ok {
		flags |= FlagHasBlob
		if p.BlobFrontCode {
			flags |= FlagBlobFrontCoded
		}
	}
	h := &Header{
		VersionMajor: VersionMajor,
		VersionMinor: VersionMinor,
		PartitionID:  p.ID,
		Flags:        flags,
		ChecksumAlgo: ChecksumCRC32C,
		DefaultCodec: codec,
		HostKeyLo:    p.HostKeyLo,
		HostKeyHi:    p.HostKeyHi,
		URLCount:     uint64(urlCount),
		HostCount:    uint64(len(p.Hosts)),
		FooterOffset: footerOff,
		CreatedHours: p.CreatedHours,
	}
	if _, err = f.WriteAt(h.Encode(), 0); err != nil {
		return err
	}
	return f.Sync()
}

// URLRecordSource yields URL records in ascending URLKey order for streaming
// region encoding. Next returns false once the records are exhausted. The source
// is pulled one record at a time, so a caller can k-way-merge the store's sorted
// shards (spec 2072 D9) without ever materializing the whole partition.
type URLRecordSource interface {
	Next() (m.URLRecord, bool)
}

// colStream is the per-column accumulator the streaming encoder keeps. It holds
// at most one page's column-major bytes (pageBuf) plus the running directory
// summary, and spills each finished page to a temp file. Memory is therefore one
// page per column, not one value per row.
type colStream struct {
	schema column // id, width, kind, enc, codec; data is unused here

	f       *os.File  // temp file holding this column's encoded pages in order
	crc     io.Writer // crc32c hash over the page span, fed each page
	sum     func() uint32
	written uint64 // bytes spilled to f so far

	pageBuf []byte // the current page's column-major bytes

	pages             []pageEntry
	colComp           uint64
	totalUncompressed uint64
	numPages          uint64
	firstEnc          uint8
	haveFirstEnc      bool

	zmin, zmax uint64 // full-column zone, aggregated from the per-page zones
	hasZone    bool
}

// StreamURLRegion encodes the URL column region by pulling records from src one
// at a time, spilling each column's pages to a temp file under tmpDir, then
// concatenating the temp files in column order to w. It produces byte-identical
// output to encodeColumnRegion(urlColumns(allRecords, codec), regionStart,
// maxRows): same page boundaries, same per-page buildColumnPage decisions, same
// directory. The win is residency. It holds at most one page per column (about
// maxRows*width bytes each) plus one record, never the O(rows) partition slice or
// the column-major copy that the materializing checkpoint builds (spec 2072 D9,
// 2071 implementation doc 51). regionStart is the region's absolute file offset so
// the directory's firstPageOffset is absolute, matching encodeColumnRegion.
//
// maxRows must be > 0 for the residency win: with maxRows <= 0 the format keeps a
// single page per column and a single page is the whole column, so the last page
// buffer holds every row. The function stays byte-identical in that case but is no
// longer bounded; the checkpoint caller passes a bounded maxRows.
// The fourth return is the CRC32C over the entire region's bytes, which the
// footer's region descriptor records exactly as Encode/EncodeToFile do for the
// materialized region.
func StreamURLRegion(w io.Writer, src URLRecordSource, regionStart uint64, maxRows int, codec uint8, tmpDir string) ([]columnDir, uint64, uint32, error) {
	schema := urlColumns(nil, codec) // 23 descriptors with the right id/width/kind/enc/codec, empty data
	cols := make([]colStream, len(schema))
	for j := range schema {
		f, err := os.CreateTemp(tmpDir, "meguri-col-*.page")
		if err != nil {
			closeAndRemove(cols[:j])
			return nil, 0, 0, err
		}
		h := crc32.New(castagnoli)
		cols[j] = colStream{schema: schema[j], f: f, crc: h, sum: h.Sum32}
	}
	defer closeAndRemove(cols)

	pageRows := 0
	firstRowOfPage := 0
	totalRows := 0

	flush := func() error {
		for j := range cols {
			if err := cols[j].flushPage(firstRowOfPage); err != nil {
				return err
			}
		}
		firstRowOfPage += pageRows
		pageRows = 0
		return nil
	}

	for {
		r, ok := src.Next()
		if !ok {
			break
		}
		for j := range cols {
			cols[j].pageBuf = appendURLField(cols[j].pageBuf, cols[j].schema.id, &r)
		}
		pageRows++
		totalRows++
		if maxRows > 0 && pageRows == maxRows {
			if err := flush(); err != nil {
				return nil, 0, 0, err
			}
		}
	}
	// A trailing partial page, or one empty page for a zero-row column, mirrors
	// splitColumn: it never emits a trailing empty page after an exact multiple.
	if pageRows > 0 || totalRows == 0 {
		if err := flush(); err != nil {
			return nil, 0, 0, err
		}
	}

	regionCRC := crc32.New(castagnoli)
	dir := make([]columnDir, len(cols))
	off := regionStart
	for j := range cols {
		c := &cols[j]
		d := columnDir{
			columnID:          c.schema.id,
			firstPageOffset:   off,
			totalCompressed:   c.colComp,
			totalUncompressed: c.totalUncompressed,
			numValues:         uint64(totalRows),
			numPages:          c.numPages,
			width:             uint8(c.schema.width),
			encoding:          c.firstEnc,
			codec:             c.schema.codec,
			columnCRC32C:      c.sum(),
			zoneMin:           c.zmin,
			zoneMax:           c.zmax,
			hasZone:           c.hasZone,
		}
		if c.numPages > 1 {
			d.pages = c.pages
		}
		dir[j] = d

		if _, err := c.f.Seek(0, io.SeekStart); err != nil {
			return nil, 0, 0, err
		}
		if _, err := io.Copy(io.MultiWriter(w, regionCRC), c.f); err != nil {
			return nil, 0, 0, err
		}
		off += c.written
	}
	return dir, off - regionStart, regionCRC.Sum32(), nil
}

// flushPage encodes the column's current page buffer exactly as encodeColumnRegion
// would (buildColumnPage picks raw vs cascade per page), spills it to the temp
// file, folds it into the running CRC and directory summary, and resets the buffer.
func (c *colStream) flushPage(firstRow int) error {
	page := c.schema
	page.data = c.pageBuf
	enc, encUsed := buildColumnPage(page, uint32(firstRow))
	if !c.haveFirstEnc {
		c.firstEnc = encUsed
		c.haveFirstEnc = true
	}
	if _, err := c.f.Write(enc); err != nil {
		return err
	}
	c.crc.Write(enc)
	c.written += uint64(len(enc))

	zmin, zmax, hasZone := zoneMap(page)
	c.pages = append(c.pages, pageEntry{
		firstRow:  uint64(firstRow),
		numValues: uint64(page.numValues()),
		byteLen:   uint64(len(enc)),
		encoding:  encUsed,
		zoneMin:   zmin,
		zoneMax:   zmax,
		hasZone:   hasZone,
	})
	c.colComp += uint64(len(enc) - PageHeaderSize)
	c.totalUncompressed += uint64(len(c.pageBuf))
	if hasZone {
		if !c.hasZone {
			c.zmin, c.zmax, c.hasZone = zmin, zmax, true
		} else {
			if zmin < c.zmin {
				c.zmin = zmin
			}
			if zmax > c.zmax {
				c.zmax = zmax
			}
		}
	}
	c.numPages++
	c.pageBuf = c.pageBuf[:0]
	return nil
}

func closeAndRemove(cols []colStream) {
	for j := range cols {
		if cols[j].f != nil {
			name := cols[j].f.Name()
			cols[j].f.Close()
			os.Remove(name)
		}
	}
}

// appendURLField appends record r's value for column id to buf in column-major
// little-endian form. It mirrors urlColumns field-for-field, so the streamed
// column bytes are identical to the materialized ones.
func appendURLField(buf []byte, id int, r *m.URLRecord) []byte {
	switch id {
	case colURLHostKey:
		return appU64(buf, r.URLKey.HostKey)
	case colURLPathKey:
		return appU64(buf, r.URLKey.PathKey)
	case colURLStatus:
		return appU8(buf, uint8(r.Status))
	case colURLPriority:
		return appF32(buf, r.Priority)
	case colURLDepth:
		return appU16(buf, r.Depth)
	case colURLDiscSource:
		return appU8(buf, uint8(r.DiscoverySource))
	case colURLRef:
		return appU64(buf, r.URLRef)
	case colURLFirstSeen:
		return appU32(buf, r.FirstSeen)
	case colURLLastCrawled:
		return appU32(buf, r.LastCrawled)
	case colURLLastChanged:
		return appU32(buf, r.LastChanged)
	case colURLNextDue:
		return appU32(buf, r.NextDue)
	case colURLLambda:
		return appF32(buf, r.Lambda)
	case colURLCrawlCount:
		return appU32(buf, r.CrawlCount)
	case colURLChangeCount:
		return appU32(buf, r.ChangeCount)
	case colURLNoChangeStk:
		return appU16(buf, r.NoChangeStreak)
	case colURLETagRef:
		return appU64(buf, r.ETagRef)
	case colURLLastModified:
		return appU32(buf, r.LastModified)
	case colURLContentFP:
		return appU64(buf, r.ContentFP)
	case colURLSimhash:
		return appU64(buf, r.Simhash)
	case colURLHTTPStatus:
		return appU16(buf, r.HTTPStatus)
	case colURLRedirectRef:
		return appU64(buf, r.RedirectRef)
	case colURLRetryCount:
		return appU8(buf, r.RetryCount)
	case colURLErrorCount:
		return appU16(buf, r.ErrorCount)
	}
	return buf
}
