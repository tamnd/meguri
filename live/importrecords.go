package live

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// RecordItem is one fully-formed URL row on its way into a rebuild: the complete
// record plus the strings its refs resolve to. It is what an import from Parquet
// yields, in contrast to bulkload's Item, which carries only a discovery (key, url,
// host) and lets the build stamp the state. Here every field is already set, so the
// rebuild is lossless.
type RecordItem struct {
	Rec  m.URLRecord
	URL  string
	Host string
	ETag string
}

// RecordSource yields RecordItems in ascending URLKey order. ImportRecords does not
// sort, so the caller must present the rows key-ordered; a meguri Parquet export
// always is, because it was written in key order.
type RecordSource interface {
	Next() (RecordItem, bool, error)
}

// ImportRecords builds a compact .meguri file from a stream of full records, the
// inverse of a Parquet export (spec 2073 doc 08, the file is the store). It mirrors
// BulkLoad's bounded-memory shape but carries the whole record rather than stamping a
// discovery: the strings intern host-clustered into the arena, the resident seen-set
// filter is solved over the keys, and the streaming columnar encoder writes the single
// output file. The transient is one record at a time plus the host map, never the whole
// table.
//
// The input must be in ascending URLKey order (a meguri export is); a key that moves
// backward is an error rather than a silently misbuilt file, because the encoder and
// the seen-set both rely on the order. Two disk scratch files carry the rows across the
// two passes the arena's hosts-before-urls layout needs; both are removed on the way out.
func ImportRecords(src RecordSource, opts BuildOptions) (BuildResult, error) {
	var res BuildResult
	if opts.FPRate <= 0 {
		opts.FPRate = 0.01
	}
	if opts.CrawlDelay == 0 {
		opts.CrawlDelay = 100
	}
	if opts.Codec == 0 {
		opts.Codec = format.CodecZstd
	}
	cap := opts.ExpectedKeys
	if cap == 0 {
		cap = 1 << 20
	}

	work, err := os.MkdirTemp(opts.TmpDir, "meguri-import-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(work)

	// Pass 1: stream the source once to a scratch temp, collecting the distinct hosts
	// and solving the seen-set filter in key order. The scratch holds each row's fixed
	// fields followed by its url and etag strings, so pass 2 can re-read it without the
	// source. The host string is captured the first time a host key appears; because
	// the input is key-ordered and keys are host-major, that is the host's first row.
	seen := newSeenBuilder(opts.FPRate, cap)
	hosts := make(map[uint64]string, 1<<16)
	scratchPath := filepath.Join(work, "scratch")
	scratchFile, err := os.Create(scratchPath)
	if err != nil {
		return res, err
	}
	sw := bufio.NewWriterSize(scratchFile, 1<<20)
	var rowBuf [rowWidth]byte
	var lenBuf [binary.MaxVarintLen64]byte
	var last m.URLKey
	have := false
	total := 0
	for {
		it, ok, e := src.Next()
		if e != nil {
			_ = scratchFile.Close()
			return res, e
		}
		if !ok {
			break
		}
		key := it.Rec.URLKey
		if have && key.Less(last) {
			_ = scratchFile.Close()
			return res, fmt.Errorf("import: rows out of key order at row %d (a meguri export is key-ordered)", total)
		}
		last, have = key, true
		if _, ok := hosts[key.HostKey]; !ok {
			hosts[key.HostKey] = it.Host
		}
		seen.addSorted(key)
		if e := writeScratch(sw, &rowBuf, lenBuf[:], &it); e != nil {
			_ = scratchFile.Close()
			return res, e
		}
		total++
	}
	if e := sw.Flush(); e != nil {
		_ = scratchFile.Close()
		return res, e
	}
	if _, e := scratchFile.Seek(0, io.SeekStart); e != nil {
		_ = scratchFile.Close()
		return res, e
	}

	// Phase between passes: intern the host strings into the arena first, in sorted
	// host-key order, so host refs sit at the arena's low end and the URL strings that
	// follow stay host-clustered. This is the layout the reader's ascending host walk
	// and a compactor both rely on.
	arena, err := newArenaWriter(filepath.Join(work, "arena"))
	if err != nil {
		_ = scratchFile.Close()
		return res, err
	}
	hostKeys := make([]uint64, 0, len(hosts))
	for hk := range hosts {
		hostKeys = append(hostKeys, hk)
	}
	slices.Sort(hostKeys)
	hostRecs := make([]m.HostRecord, 0, len(hostKeys))
	hostRef := make(map[uint64]uint64, len(hostKeys))
	for _, hk := range hostKeys {
		ref, e := arena.intern(hosts[hk])
		if e != nil {
			_ = arena.close()
			_ = scratchFile.Close()
			return res, e
		}
		hostRef[hk] = ref
		hostRecs = append(hostRecs, m.HostRecord{
			HostKey:    hk,
			HostRef:    ref,
			Grouping:   m.GroupFullHost,
			CrawlDelay: opts.CrawlDelay,
		})
	}
	var hostKeyLo, hostKeyHi uint64
	if len(hostKeys) > 0 {
		hostKeyLo, hostKeyHi = hostKeys[0], hostKeys[len(hostKeys)-1]
	}

	// Pass 2: re-read the scratch in key order, intern each row's url and etag after the
	// hosts (so their refs ascend), patch the refs into the record, and write the
	// key-ordered records temp the encoder streams. HostKey is refilled from the key.
	recPath := filepath.Join(work, "records")
	recFile, err := os.Create(recPath)
	if err != nil {
		_ = arena.close()
		_ = scratchFile.Close()
		return res, err
	}
	recW := bufio.NewWriterSize(recFile, 1<<20)
	sr := bufio.NewReaderSize(scratchFile, 1<<20)
	for {
		rec, url, etag, ok, e := readScratch(sr, &rowBuf)
		if e != nil {
			_ = closeAll(arena, scratchFile, recFile)
			return res, e
		}
		if !ok {
			break
		}
		ref, e := arena.intern(url)
		if e != nil {
			_ = closeAll(arena, scratchFile, recFile)
			return res, e
		}
		rec.URLRef = ref
		if etag != "" {
			er, e := arena.intern(etag)
			if e != nil {
				_ = closeAll(arena, scratchFile, recFile)
				return res, e
			}
			rec.ETagRef = er
		} else {
			rec.ETagRef = 0
		}
		rec.HostKey = rec.URLKey.HostKey
		encodeRow(rowBuf[:], &rec)
		if _, e := recW.Write(rowBuf[:]); e != nil {
			_ = closeAll(arena, scratchFile, recFile)
			return res, e
		}
	}
	_ = scratchFile.Close()
	if e := recW.Flush(); e != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, e
	}
	if _, e := recFile.Seek(0, io.SeekStart); e != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, e
	}
	if e := arena.flush(); e != nil {
		_ = recFile.Close()
		return res, e
	}

	// Phase 3: streaming columnar encode, identical to BulkLoad's tail. The records
	// temp feeds records in key order; the arena is the string region; the seen-set
	// filter is solved once over the keys pass 1 collected.
	filterBytes, bitsPerURL, err := seen.marshal()
	if err != nil {
		_ = recFile.Close()
		return res, err
	}
	p := &format.Partition{
		ID:            opts.PartitionID,
		HostKeyLo:     hostKeyLo,
		HostKeyHi:     hostKeyHi,
		CreatedHours:  opts.NowHours,
		DefaultCodec:  opts.Codec,
		Hosts:         hostRecs,
		StringsAt:     arena.file(),
		StringsSize:   arena.size(),
		SeenFilter:    filterBytes,
		MaxPageRows:   opts.PageRows,
		BlobFrontCode: true,
	}
	source := &recordSource{r: bufio.NewReaderSize(recFile, 1<<20)}
	encErr := format.StreamEncodeToFile(opts.Path, source, opts.PageRows, p, work)
	_ = recFile.Close()
	_ = arena.close()
	if encErr != nil {
		return res, encErr
	}
	if source.err != nil {
		return res, source.err
	}

	fi, err := os.Stat(opts.Path)
	if err != nil {
		return res, err
	}
	res = BuildResult{
		URLCount:   total,
		HostCount:  len(hostRecs),
		FileBytes:  fi.Size(),
		BitsPerURL: bitsPerURL,
	}
	return res, nil
}

// writeScratch appends one row to the pass-1 scratch: the fixed record fields, then the
// url and etag strings length-prefixed. The record's ref fields are placeholders here;
// pass 2 overwrites them with the arena offsets.
func writeScratch(w *bufio.Writer, rowBuf *[rowWidth]byte, lenBuf []byte, it *RecordItem) error {
	rec := it.Rec
	rec.HostKey = rec.URLKey.HostKey
	encodeRow(rowBuf[:], &rec)
	if _, err := w.Write(rowBuf[:]); err != nil {
		return err
	}
	if err := writeString(w, lenBuf, it.URL); err != nil {
		return err
	}
	return writeString(w, lenBuf, it.ETag)
}

// writeString writes a uvarint length then the bytes.
func writeString(w *bufio.Writer, lenBuf []byte, s string) error {
	n := binary.PutUvarint(lenBuf, uint64(len(s)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return err
	}
	_, err := w.WriteString(s)
	return err
}

// readScratch reads back one row writeScratch wrote. ok is false at EOF.
func readScratch(r *bufio.Reader, rowBuf *[rowWidth]byte) (rec m.URLRecord, url, etag string, ok bool, err error) {
	if _, e := io.ReadFull(r, rowBuf[:]); e != nil {
		if e == io.EOF {
			return rec, "", "", false, nil
		}
		return rec, "", "", false, e
	}
	rec = decodeRow(rowBuf[:])
	url, err = readString(r)
	if err != nil {
		return rec, "", "", false, err
	}
	etag, err = readString(r)
	if err != nil {
		return rec, "", "", false, err
	}
	return rec, url, etag, true, nil
}

// readString reads a uvarint-prefixed string.
func readString(r *bufio.Reader) (string, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// closeAll closes the three open handles an import error path holds, ignoring the
// errors since the original failure is what the caller returns.
func closeAll(a *arenaWriter, scratch, rec *os.File) error {
	_ = a.close()
	_ = scratch.Close()
	_ = rec.Close()
	return nil
}
