package live

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"slices"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/format"
)

// CompactOptions configures a Stage 2 compaction.
type CompactOptions struct {
	OutPath     string  // the new .meguri file; written to a temp then atomically renamed
	TmpDir      string  // scratch for the new arena and the records temp
	PageRows    int     // encoder page row cap (match the base for a stable layout)
	Codec       uint8   // format.CodecZstd
	FPRate      float64 // filter FP budget when the base carries no filter to reuse
	NowHours    uint32  // stamped on delta inserts that leave FirstSeen/NextDue unset
	PartitionID uint32
	CrawlDelay  uint16 // crawl delay for hosts the delta introduces (0 -> 100)
}

// CompactResult reports what a compaction produced.
type CompactResult struct {
	URLCount   int
	HostCount  int
	Inserted   int // delta keys not present in the base
	Updated    int // delta keys that replaced a base row
	Carried    int // base rows copied through unchanged
	FileBytes  int64
	BitsPerURL float64
}

// Compact folds a resident Delta into the base .meguri file and writes the next file
// generation, the write path of spec 2073 doc 08 Stage 2. It merge-joins the base URL
// table (read in URLKey order through a row cursor) with the sorted delta: a delta key
// that matches a base row replaces it (a recrawl update), a delta key with no match is
// inserted, and every untouched base row is carried through. The base's URL and host
// strings are re-interned into a fresh arena, so the output is one self-contained file
// with no reference back to the base.
//
// The read side is sequential, not random: the cursor walks base rows in key order and
// BulkLoad interned strings in that order, so an ArenaSeqReader resolves each carried
// row's string with about one blob page resident, never the whole arena. This is why
// compaction is the cursor-based merge the doc argues for over the random point-lookup
// tail the 100M rediscovery sample measured. The transient is the host table, the
// resident filter, one blob-page window, the sorted delta, plus one encoder page per
// column; the durable output is a single file and the temps are removed on the way out.
func Compact(basePath string, delta *Delta, opts CompactOptions) (CompactResult, error) {
	var res CompactResult
	if opts.Codec == 0 {
		opts.Codec = format.CodecZstd
	}
	if opts.FPRate <= 0 {
		opts.FPRate = 0.01
	}
	if opts.CrawlDelay == 0 {
		opts.CrawlDelay = 100
	}

	fileBytes, closeMap, err := mmapFile(basePath)
	if err != nil {
		return res, err
	}
	defer closeMap()
	adviseRandom(fileBytes)
	base, err := format.NewReader(fileBytes)
	if err != nil {
		return res, err
	}

	work, err := os.MkdirTemp(opts.TmpDir, "meguri-compact-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(work)

	entries := delta.sorted()

	// The seen filter of the new file is the base filter with the delta's keys added,
	// so no full base rescan is needed to rebuild it. A base without a filter (not a
	// BulkLoad output) falls back to a fresh filter sized for the merged count.
	filter, err := loadOrBuildFilter(base, len(entries), opts.FPRate)
	if err != nil {
		return res, err
	}
	for i := range entries {
		filter.Add(entries[i].Rec.URLKey)
	}

	// One sequential arena reader serves the whole read: host strings sit at the low
	// end of the base arena (BulkLoad interns hosts before URLs) and the URL strings
	// follow, both in ascending ref order, so a single forward pass resolves hosts
	// first then URLs.
	arenaSeq := base.ArenaSeqReader()

	// Phase 1: the host table of the new file is the union of the base hosts and the
	// hosts the delta introduces. Resolve the base host strings through the sequential
	// reader in ascending ref order, then merge the delta's new hosts.
	arena, err := newArenaWriter(filepath.Join(work, "arena"))
	if err != nil {
		return res, err
	}
	hostRecs, hostKeyLo, hostKeyHi, err := buildHostTable(base, delta, arenaSeq, arena, opts)
	if err != nil {
		_ = arena.close()
		return res, err
	}

	// Phase 2: merge-join the base rows and the sorted delta in key order, interning
	// each emitted row's URL into the arena (after the hosts) and writing the fixed
	// width record to the encoder's temp. Both writes are sequential.
	recPath := filepath.Join(work, "records")
	recFile, err := os.Create(recPath)
	if err != nil {
		_ = arena.close()
		return res, err
	}
	recW := bufio.NewWriterSize(recFile, 1<<20)

	cur, err := base.URLRows()
	if err != nil {
		_ = arena.close()
		_ = recFile.Close()
		return res, err
	}

	mj := &mergeJoin{cur: cur, delta: entries, arena: arena, seq: arenaSeq, opts: opts}
	var rowBuf [rowWidth]byte
	for {
		rec, ok, e := mj.next()
		if e != nil {
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		if !ok {
			break
		}
		encodeRow(rowBuf[:], &rec)
		if _, e := recW.Write(rowBuf[:]); e != nil {
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
	}
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

	// Phase 3: streaming columnar encode into a temp file, then an atomic rename so a
	// reader never sees a half-written generation.
	tmpOut := opts.OutPath + ".tmp"
	p := &format.Partition{
		ID:           opts.PartitionID,
		HostKeyLo:    hostKeyLo,
		HostKeyHi:    hostKeyHi,
		CreatedHours: opts.NowHours,
		DefaultCodec: opts.Codec,
		Hosts:        hostRecs,
		StringsAt:    arena.file(),
		StringsSize:  arena.size(),
		SeenFilter:   filter.Marshal(),
		MaxPageRows:  opts.PageRows,
	}
	source := &recordSource{r: bufio.NewReaderSize(recFile, 1<<20)}
	encErr := format.StreamEncodeToFile(tmpOut, source, opts.PageRows, p, work)
	_ = recFile.Close()
	_ = arena.close()
	if encErr != nil {
		_ = os.Remove(tmpOut)
		return res, encErr
	}
	if source.err != nil {
		_ = os.Remove(tmpOut)
		return res, source.err
	}
	if err := os.Rename(tmpOut, opts.OutPath); err != nil {
		_ = os.Remove(tmpOut)
		return res, err
	}

	fi, err := os.Stat(opts.OutPath)
	if err != nil {
		return res, err
	}
	res = CompactResult{
		URLCount:   mj.emitted,
		HostCount:  len(hostRecs),
		Inserted:   mj.inserted,
		Updated:    mj.updated,
		Carried:    mj.carried,
		FileBytes:  fi.Size(),
		BitsPerURL: filter.BitsPerURL(),
	}
	return res, nil
}

// loadOrBuildFilter returns the new file's seen filter. It reuses the base's filter
// region when present (the common case, a BulkLoad or Compact output), so the merged
// file's filter is the base's plus the delta keys the caller adds. When the base has
// no filter region the filter is rebuilt by scanning the base keys, sized for the
// merged count.
func loadOrBuildFilter(base *format.Reader, deltaKeys int, fp float64) (*dedup.Filter, error) {
	fb, err := base.SeenFilter()
	if err != nil {
		return nil, err
	}
	if len(fb) > 0 {
		return dedup.LoadFilter(fb)
	}
	filter := dedup.NewFilter(uint64(base.URLCount()+deltaKeys), fp)
	cur, err := base.URLRows()
	if err != nil {
		return nil, err
	}
	for {
		rec, ok, e := cur.Next()
		if e != nil {
			return nil, e
		}
		if !ok {
			break
		}
		filter.Add(rec.URLKey)
	}
	return filter, nil
}

// buildHostTable interns the union of the base and delta hosts into the new arena and
// returns the host records with their fresh refs, plus the hostkey span. Base host
// strings resolve through the sequential arena reader in ascending ref order (their
// refs precede every URL ref), and the delta's new hosts intern after them.
func buildHostTable(base *format.Reader, delta *Delta, seq *format.ArenaSeqReader, arena *arenaWriter, opts CompactOptions) ([]m.HostRecord, uint64, uint64, error) {
	baseHosts, err := base.Hosts()
	if err != nil {
		return nil, 0, 0, err
	}
	// Resolve base host strings in ascending ref order so the single sequential reader
	// stays forward-only. Keep the base HostRecord fields (grouping, crawl delay).
	byRef := slices.Clone(baseHosts)
	slices.SortFunc(byRef, func(a, b m.HostRecord) int {
		switch {
		case a.HostRef < b.HostRef:
			return -1
		case a.HostRef > b.HostRef:
			return 1
		default:
			return 0
		}
	})
	strs := make(map[uint64]string, len(baseHosts))
	tmpl := make(map[uint64]m.HostRecord, len(baseHosts))
	for _, h := range byRef {
		s, e := seq.At(h.HostRef)
		if e != nil {
			return nil, 0, 0, e
		}
		strs[h.HostKey] = string(s)
		tmpl[h.HostKey] = h
	}
	// The delta may introduce hosts the base does not have. Their strings come from
	// the delta entries, not the base arena.
	for hk, hs := range delta.hosts {
		if _, ok := strs[hk]; !ok {
			strs[hk] = hs
		}
	}

	hostKeys := make([]uint64, 0, len(strs))
	for hk := range strs {
		hostKeys = append(hostKeys, hk)
	}
	slices.Sort(hostKeys)

	recs := make([]m.HostRecord, 0, len(hostKeys))
	for _, hk := range hostKeys {
		ref, e := arena.intern(strs[hk])
		if e != nil {
			return nil, 0, 0, e
		}
		if h, ok := tmpl[hk]; ok {
			h.HostRef = ref
			recs = append(recs, h)
		} else {
			recs = append(recs, m.HostRecord{
				HostKey:    hk,
				HostRef:    ref,
				Grouping:   m.GroupFullHost,
				CrawlDelay: opts.CrawlDelay,
			})
		}
	}
	var lo, hi uint64
	if len(hostKeys) > 0 {
		lo, hi = hostKeys[0], hostKeys[len(hostKeys)-1]
	}
	return recs, lo, hi, nil
}

// mergeJoin yields the merged URL rows in ascending key order: the base rows from the
// cursor and the sorted delta, with the delta winning on a key match. It interns each
// emitted row's URL into the arena as it goes, so URLRef points into the new file.
type mergeJoin struct {
	cur   *format.URLRowCursor
	delta []DeltaEntry
	di    int
	arena *arenaWriter
	seq   *format.ArenaSeqReader
	opts  CompactOptions

	pending     m.URLRecord
	havePending bool

	emitted  int
	inserted int
	updated  int
	carried  int
}

// next returns the next merged record. It compares the head of the base cursor with
// the head of the delta and emits the smaller key, resolving a tie in the delta's
// favor and advancing the base past the replaced row.
func (mj *mergeJoin) next() (m.URLRecord, bool, error) {
	// Refill the one-row base lookahead.
	if !mj.havePending {
		rec, ok, err := mj.cur.Next()
		if err != nil {
			return m.URLRecord{}, false, err
		}
		mj.pending, mj.havePending = rec, ok
	}
	for {
		haveDelta := mj.di < len(mj.delta)
		switch {
		case !mj.havePending && !haveDelta:
			return m.URLRecord{}, false, nil
		case mj.havePending && (!haveDelta || mj.pending.URLKey.Compare(mj.delta[mj.di].Rec.URLKey) < 0):
			// Base row with no delta at or before it: carry it through, re-interning
			// its URL from the base arena into the new one.
			rec := mj.pending
			s, err := mj.seq.At(rec.URLRef)
			if err != nil {
				return m.URLRecord{}, false, err
			}
			ref, err := mj.arena.intern(string(s))
			if err != nil {
				return m.URLRecord{}, false, err
			}
			rec.URLRef = ref
			// Outcome-string refs point into the base arena and are not re-interned in
			// this generation; a recrawl re-establishes them through the delta.
			rec.ETagRef, rec.RedirectRef = 0, 0
			mj.havePending = false
			mj.carried++
			mj.emitted++
			return rec, true, nil
		default:
			// A delta entry that is either strictly ahead of the base head (an insert)
			// or equal to it (an update that replaces the base row).
			e := mj.delta[mj.di]
			mj.di++
			if mj.havePending && mj.pending.URLKey == e.Rec.URLKey {
				mj.havePending = false // drop the replaced base row
				mj.updated++
			} else {
				mj.inserted++
			}
			rec := e.Rec
			ref, err := mj.arena.intern(e.URL)
			if err != nil {
				return m.URLRecord{}, false, err
			}
			rec.URLRef = ref
			rec.HostKey = rec.URLKey.HostKey
			if rec.FirstSeen == 0 {
				rec.FirstSeen = mj.opts.NowHours
			}
			if rec.NextDue == 0 {
				rec.NextDue = mj.opts.NowHours
			}
			mj.emitted++
			return rec, true, nil
		}
	}
}
