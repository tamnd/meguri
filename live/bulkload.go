package live

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"slices"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// Item is one URL on its way into a bulk build: its key, its canonical string, and
// its host grouping. The caller owns canonicalization and keying (the scale harness
// and the engine both have it), so the loader stays a pure sort-and-encode.
type Item struct {
	Key  m.URLKey
	URL  string
	Host string // host grouping string, interned once per distinct host
}

// Source yields discovered items for a bulk build. Next returns ok false at the
// end. It is pulled once, streaming, so the corpus is never resident.
type Source interface {
	Next() (Item, bool, error)
}

// BuildOptions configures a bulk build.
type BuildOptions struct {
	Path         string  // output .meguri file
	TmpDir       string  // scratch for sort runs, the arena, and the records temp
	ExpectedKeys uint64  // sizes the resident filter; the corpus row count is exact
	RunRows      int     // sort buffer cap in rows (0 = 1<<20)
	PageRows     int     // encoder page row cap (0 = single page, not for scale)
	Codec        uint8   // format.CodecZstd for the compact file
	FPRate       float64 // filter false-positive budget (0 = 1%)
	NowHours     uint32  // epoch-hours stamped as FirstSeen and NextDue
	PartitionID  uint32
	Priority     float32     // default URL priority (0 -> 0.5)
	Status       m.URLStatus // default URL status
	Source       m.DiscoverySource
	CrawlDelay   uint16 // host crawl delay, deciseconds (0 -> 100)
}

// BuildResult reports what a bulk build produced.
type BuildResult struct {
	URLCount   int
	HostCount  int
	FileBytes  int64
	BitsPerURL float64
}

// BulkLoad builds one compact .meguri file from a discovery source in bounded
// memory (spec 2073 doc 08, the friendly bulk case). It external-sorts the items
// by URLKey, builds the resident seen-set filter and the host table, writes the
// string arena host-clustered to a temp file, and runs the streaming columnar
// encode from a key-ordered records temp, so the durable output is a single file
// and the transient is one sort buffer plus one encoder page per column. No DRUM,
// no append log, no permanent arena: the temp files are removed on the way out.
func BulkLoad(src Source, opts BuildOptions) (BuildResult, error) {
	var res BuildResult
	if opts.RunRows <= 0 {
		opts.RunRows = 1 << 20
	}
	if opts.FPRate <= 0 {
		opts.FPRate = 0.01
	}
	if opts.Priority == 0 {
		opts.Priority = 0.5
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

	work, err := os.MkdirTemp(opts.TmpDir, "meguri-bulk-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(work)

	// Phase 1: stream the source into sorted runs and collect the distinct hosts.
	// This is the only pass that sees the raw source, so it does all the per-item
	// work the later phases must not repeat. The seen-set keys are collected in the
	// sorted phase-2 merge instead of here: the ribbon (dedup/ribbon.go) is a static
	// filter solved once over the distinct key set, and the merge hands the keys up
	// already ordered so dups drop in place.
	seen, err := newSeenBuilder(opts.FPRate, cap, work)
	if err != nil {
		return res, err
	}
	hosts := make(map[uint64]string, 1<<16)
	rb := newRunBuilder(work, opts.RunRows)
	for {
		it, ok, e := src.Next()
		if e != nil {
			return res, e
		}
		if !ok {
			break
		}
		if _, seen := hosts[it.Key.HostKey]; !seen {
			hosts[it.Key.HostKey] = it.Host
		}
		if e := rb.add(it.Key, it.URL); e != nil {
			return res, e
		}
	}
	runs, err := rb.finish()
	if err != nil {
		return res, err
	}

	// Phase 1.5: freeze the filter, and write the host strings to the arena first so
	// the host refs are known before the encode. The arena stays open as the
	// encoder's StringsAt; URL strings append after the hosts in phase 2.
	arena, err := newArenaWriter(filepath.Join(work, "arena"))
	if err != nil {
		return res, err
	}
	hostKeys := make([]uint64, 0, len(hosts))
	for hk := range hosts {
		hostKeys = append(hostKeys, hk)
	}
	slices.Sort(hostKeys)
	hostRecs := make([]m.HostRecord, 0, len(hostKeys))
	for _, hk := range hostKeys {
		ref, e := arena.intern(hosts[hk])
		if e != nil {
			_ = arena.close()
			return res, e
		}
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

	// Phase 2: merge the runs in key order. Each URL string interns into the arena
	// (now host-clustered, after the hosts) and the record, with its ref, is written
	// to the key-ordered records temp the encoder reads. Both writes are sequential.
	recPath := filepath.Join(work, "records")
	recFile, err := os.Create(recPath)
	if err != nil {
		_ = arena.close()
		return res, err
	}
	recW := bufio.NewWriterSize(recFile, 1<<20)
	next, closeRuns, err := mergeRuns(runs)
	if err != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, err
	}
	var rowBuf [rowWidth]byte
	urlCount := 0
	for {
		it, ok, e := next()
		if e != nil {
			_ = closeRuns()
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		if !ok {
			break
		}
		if e := seen.addSorted(it.key); e != nil {
			_ = closeRuns()
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		ref, e := arena.intern(it.url)
		if e != nil {
			_ = closeRuns()
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		rec := m.URLRecord{
			URLKey:          it.key,
			HostKey:         it.key.HostKey,
			Status:          opts.Status,
			Priority:        opts.Priority,
			URLRef:          ref,
			FirstSeen:       opts.NowHours,
			NextDue:         opts.NowHours,
			DiscoverySource: opts.Source,
		}
		encodeRow(rowBuf[:], &rec)
		if _, e := recW.Write(rowBuf[:]); e != nil {
			_ = closeRuns()
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		urlCount++
	}
	if e := closeRuns(); e != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, e
	}
	if e := recW.Flush(); e != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, e
	}
	// The records temp is read back from the start as the encoder's source; rewind
	// it rather than reopen.
	if _, e := recFile.Seek(0, io.SeekStart); e != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, e
	}
	if e := arena.flush(); e != nil {
		_ = recFile.Close()
		return res, e
	}

	// Phase 3: streaming columnar encode. The source feeds records in key order from
	// the records temp; the arena temp is the string region read through StringsAt.
	// The seen-set filter is solved here, once, over the keys the merge collected.
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
		URLCount:   urlCount,
		HostCount:  len(hostRecs),
		FileBytes:  fi.Size(),
		BitsPerURL: bitsPerURL,
	}
	return res, nil
}

// recordSource reads the key-ordered records temp back as a format.URLRecordSource.
// A read error past the first record stops the stream and is surfaced after the
// encode, since URLRecordSource.Next has no error return.
type recordSource struct {
	r   *bufio.Reader
	buf [rowWidth]byte
	err error
}

func (s *recordSource) Next() (m.URLRecord, bool) {
	if s.err != nil {
		return m.URLRecord{}, false
	}
	if _, err := io.ReadFull(s.r, s.buf[:]); err != nil {
		if err != io.EOF {
			s.err = err
		}
		return m.URLRecord{}, false
	}
	return decodeRow(s.buf[:]), true
}
