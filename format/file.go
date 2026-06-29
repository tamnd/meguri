package format

import m "github.com/tamnd/meguri"

// trailerSize is the fixed tail every file ends with: footer_length u32,
// footer_crc32c u32, end magic.
const trailerSize = 12

// Encode serializes a Partition into a complete .meguri file. The output is
// deterministic: the same Partition value always produces the same bytes, which
// is what makes a checkpoint diffable and the round-trip gate meaningful. Encode
// does not sort; it returns ErrNotSorted if the caller's rows are out of order.
func Encode(p *Partition) ([]byte, error) {
	if !sortedURLs(p.URLs) {
		return nil, ErrNotSorted
	}
	if !sortedHosts(p.Hosts) {
		return nil, ErrNotSorted
	}

	codec := p.DefaultCodec

	// Lay the regions out after the header, tracking absolute offsets so the
	// column directories can address pages from the start of the file.
	pos := uint64(HeaderSize)

	urlRegion, urlDir := encodeColumnRegion(urlColumns(p.URLs, codec), pos, p.MaxPageRows)
	urlOff := pos
	pos += uint64(len(urlRegion))

	hostRegion, hostDir := encodeColumnRegion(hostColumns(p.Hosts, codec), pos, p.MaxPageRows)
	hostOff := pos
	pos += uint64(len(hostRegion))

	// The schedule index region sits after the host table (doc 10 section 2, the
	// region order: url, host, schedule, seenset, blob). It is present only when the
	// caller asked for the wheel and at least one row is scheduled.
	var schedRegion []byte
	if p.BuildSchedule {
		schedRegion = encodeScheduleRegion(p.URLs, codec)
	}
	schedOff := pos
	pos += uint64(len(schedRegion))

	// The seen-set filter region sits between the schedule index and the string
	// blob. It is present only when the caller carried a serialized filter into the
	// partition.
	seenRegion := encodeSeensetRegion(p.SeenFilter, uint64(len(p.URLs)), codec)
	seenOff := pos
	pos += uint64(len(seenRegion))

	blobRegion := encodeBlobRegion(p.Strings, codec)
	strOff := pos
	pos += uint64(len(blobRegion))

	footerOff := pos

	regions := []regionDesc{
		{id: RegionURLTable, offset: urlOff, length: uint64(len(urlRegion)), crc: crc32c(urlRegion)},
		{id: RegionHostTable, offset: hostOff, length: uint64(len(hostRegion)), crc: crc32c(hostRegion)},
	}
	if len(schedRegion) > 0 {
		regions = append(regions, regionDesc{
			id: RegionSchedule, offset: schedOff, length: uint64(len(schedRegion)), crc: crc32c(schedRegion),
		})
	}
	if len(seenRegion) > 0 {
		regions = append(regions, regionDesc{
			id: RegionSeenset, offset: seenOff, length: uint64(len(seenRegion)), crc: crc32c(seenRegion),
		})
	}
	if len(blobRegion) > 0 {
		regions = append(regions, regionDesc{
			id: RegionStringBlob, offset: strOff, length: uint64(len(blobRegion)), crc: crc32c(blobRegion),
		})
	}

	totalComp := dirTotals(urlDir) + dirTotals(hostDir)
	totalUncomp := uncompTotals(urlDir) + uncompTotals(hostDir)
	dueMin, dueMax := dueRange(p.URLs)

	stats := statsBlock{
		urlCount:          uint64(len(p.URLs)),
		hostCount:         uint64(len(p.Hosts)),
		hostKeyLo:         p.HostKeyLo,
		hostKeyHi:         p.HostKeyHi,
		dueMin:            dueMin,
		dueMax:            dueMax,
		totalCompressed:   totalComp,
		totalUncompressed: totalUncomp,
	}
	if len(p.URLs) > 0 {
		stats.bytesPerURL = float32(footerOff) / float32(len(p.URLs))
	}

	footer := &footerData{
		regions: regions,
		urlDir:  urlDir,
		hostDir: hostDir,
		stats:   stats,
		meta:    metaPairs(p.Meta),
	}
	footerBytes := encodeFooter(footer)

	flags := FlagSorted
	if len(schedRegion) > 0 {
		flags |= FlagHasSchedule
	}
	if len(seenRegion) > 0 {
		flags |= FlagHasSeenset
	}
	if len(p.Strings) > 0 {
		flags |= FlagHasBlob
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
		URLCount:     uint64(len(p.URLs)),
		HostCount:    uint64(len(p.Hosts)),
		FooterOffset: footerOff,
		CreatedHours: p.CreatedHours,
	}

	out := make([]byte, 0, int(footerOff)+len(footerBytes)+trailerSize)
	out = append(out, h.Encode()...)
	out = append(out, urlRegion...)
	out = append(out, hostRegion...)
	out = append(out, schedRegion...)
	out = append(out, seenRegion...)
	out = append(out, blobRegion...)
	out = append(out, footerBytes...)

	var tail wbuf
	tail.u32(uint32(len(footerBytes)))
	tail.u32(crc32c(footerBytes))
	tail.bytes(Magic[:])
	out = append(out, tail.b...)
	return out, nil
}

// Decode parses a complete .meguri file back into a Partition, verifying the
// header, footer, region, page, and column checksums along the way.
func Decode(b []byte) (*Partition, error) {
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

	if err := verifyRegions(b, f.regions); err != nil {
		return nil, err
	}

	urlCols, err := decodeColumnRegion(b, f.urlDir)
	if err != nil {
		return nil, err
	}
	hostCols, err := decodeColumnRegion(b, f.hostDir)
	if err != nil {
		return nil, err
	}

	urls, err := urlRecordsFromColumns(urlCols, int(h.URLCount))
	if err != nil {
		return nil, err
	}
	hosts, err := hostRecordsFromColumns(hostCols, int(h.HostCount))
	if err != nil {
		return nil, err
	}

	p := &Partition{
		ID:           h.PartitionID,
		HostKeyLo:    h.HostKeyLo,
		HostKeyHi:    h.HostKeyHi,
		CreatedHours: h.CreatedHours,
		DefaultCodec: h.DefaultCodec,
		URLs:         urls,
		Hosts:        hosts,
		Meta:         metaMap(f.meta),
	}
	if _, ok := findRegion(f.regions, RegionSchedule); ok {
		// The wheel is derived from next_due, so a decode does not rebuild it into
		// the partition; recording that it was present lets a re-encode reproduce it
		// byte for byte.
		p.BuildSchedule = true
	}
	if reg, ok := findRegion(f.regions, RegionSeenset); ok {
		blob, err := decodeSeensetRegion(b[reg.offset : reg.offset+reg.length])
		if err != nil {
			return nil, err
		}
		p.SeenFilter = blob
	}
	if reg, ok := findRegion(f.regions, RegionStringBlob); ok {
		arena, err := decodeBlobRegion(b[reg.offset : reg.offset+reg.length])
		if err != nil {
			return nil, err
		}
		p.Strings = arena
	}
	if !sortedURLs(p.URLs) {
		return nil, ErrNotSorted
	}
	return p, nil
}

// verifyRegions checks every region descriptor's bounds and CRC against the file.
func verifyRegions(b []byte, regions []regionDesc) error {
	for _, reg := range regions {
		end := reg.offset + reg.length
		if reg.offset > uint64(len(b)) || end > uint64(len(b)) || end < reg.offset {
			return ErrCorrupt
		}
		if crc32c(b[reg.offset:end]) != reg.crc {
			return ErrChecksum
		}
	}
	return nil
}

func findRegion(regions []regionDesc, id uint8) (regionDesc, bool) {
	for _, reg := range regions {
		if reg.id == id {
			return reg, true
		}
	}
	return regionDesc{}, false
}

func dirTotals(dir []columnDir) uint64 {
	var t uint64
	for _, d := range dir {
		t += d.totalCompressed
	}
	return t
}

func uncompTotals(dir []columnDir) uint64 {
	var t uint64
	for _, d := range dir {
		t += d.totalUncompressed
	}
	return t
}

// dueRange returns the smallest and largest NextDue across the URL rows,
// ignoring the zero sentinel (a row with no scheduled crawl).
func dueRange(recs []m.URLRecord) (min, max uint32) {
	for i := range recs {
		d := recs[i].NextDue
		if d == 0 {
			continue
		}
		if min == 0 || d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	return min, max
}

func metaPairs(meta map[string]string) []kvPair {
	if len(meta) == 0 {
		return nil
	}
	out := make([]kvPair, 0, len(meta))
	for k, v := range meta {
		out = append(out, kvPair{key: k, val: v})
	}
	return out
}

func metaMap(pairs []kvPair) map[string]string {
	if len(pairs) == 0 {
		return nil
	}
	out := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		out[kv.key] = kv.val
	}
	return out
}
