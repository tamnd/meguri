package live

import (
	"bufio"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/freshness"
)

// RecrawlOptions configures a Stage 3 recrawl fold.
type RecrawlOptions struct {
	OutPath     string  // the new .meguri file; written to a temp then atomically renamed
	TmpDir      string  // scratch for the new arena and the records temp
	PageRows    int     // encoder page row cap (match the base for a stable layout)
	Codec       uint8   // format.CodecZstd
	NowHours    uint32  // a row with 0 < NextDue <= NowHours is due and gets an outcome folded
	PartitionID uint32  // partition id stamped on the new file
	FPRate      float64 // filter FP budget when the base carries no filter to reuse
	CrawlDelay  uint16  // crawl delay for hosts the base carries with none (0 -> 100)

	Params     freshness.Params // recrawler math parameters (freshness.DefaultParams when zero)
	Tau        float64          // the water level the allocation reschedules against
	ChangeRate float64          // probability an outcome is a real content change, the rest are 304 no-change
	Seed       uint64           // deterministic outcome draw seed
}

// RecrawlResult reports what a recrawl fold produced.
type RecrawlResult struct {
	URLCount   int
	HostCount  int
	Recrawled  int     // due rows that had an outcome folded and were rescheduled
	Carried    int     // rows not due, copied through unchanged
	Changed    int     // outcomes classified as a real content change
	NoChange   int     // outcomes that were a 304 or a cosmetic no-change
	FileBytes  int64   // the new generation's size on disk
	MeanLambda float64 // mean estimated change rate over the recrawled rows, a convergence sanity
	BitsPerURL float64
}

// Recrawl folds a crawl outcome into every due row of the base .meguri file and writes
// the next file generation, the "update a URL after it is fetched" need of spec 2073 doc
// 08. It streams the base URL table in key order through a row cursor, and for a row whose
// NextDue is at or before now it draws a typed outcome, advances the row's change-rate
// counters exactly as the frontier's markCrawled does, re-estimates the URL's Poisson
// change rate, and sets the next due time from the water-filling allocation. A row that is
// not due is carried through unchanged. Every row's URL string is re-interned into a fresh
// arena, so the output is one self-contained generation swapped in with an atomic rename.
//
// The read is sequential, the same cursor walk compaction uses: the dispatch order the
// scheduler emits is the file's stored key order, so folding outcomes back in reads the
// base once front to back with about one blob page resident, never the random point-lookup
// tail a per-key GetURL would pay. The outcomes here are typed feedback values drawn to a
// change rate, not live fetches, so this measures the recrawler fold and its residency at
// scale, exactly the honest framing doc 05 sets for the recrawl benchmark.
func Recrawl(basePath string, opts RecrawlOptions) (RecrawlResult, error) {
	var res RecrawlResult
	if opts.Codec == 0 {
		opts.Codec = format.CodecZstd
	}
	if opts.FPRate <= 0 {
		opts.FPRate = 0.01
	}
	if opts.CrawlDelay == 0 {
		opts.CrawlDelay = 100
	}
	if opts.Params == (freshness.Params{}) {
		opts.Params = freshness.DefaultParams()
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

	work, err := os.MkdirTemp(opts.TmpDir, "meguri-recrawl-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(work)

	// The recrawl introduces no keys, so the new file's filter is the base's unchanged.
	filter, err := loadOrBuildFilter(base, 0, opts.FPRate)
	if err != nil {
		return res, err
	}

	// One sequential arena reader serves the whole read: host strings sit at the low end
	// of the base arena and the URL strings follow, both in ascending ref order.
	arenaSeq := base.ArenaSeqReader()

	// Phase 1: the host table is the base's hosts, re-interned into the new arena. The
	// records keep their crawl delay and URL count, which the allocation reads to cap a
	// URL's funded rate at its host's politeness share.
	arena, err := newArenaWriter(filepath.Join(work, "arena"))
	if err != nil {
		return res, err
	}
	hostRecs, hostKeyLo, hostKeyHi, err := buildHostTable(base, NewDelta(), arenaSeq, arena, CompactOptions{CrawlDelay: opts.CrawlDelay})
	if err != nil {
		_ = arena.close()
		return res, err
	}
	hostByKey := make(map[uint64]m.HostRecord, len(hostRecs))
	for _, h := range hostRecs {
		hostByKey[h.HostKey] = h
	}

	// Phase 2: stream the base rows in key order, folding an outcome into each due row and
	// carrying the rest, interning every emitted row's URL into the arena after the hosts.
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

	rng := rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x9e3779b97f4a7c15))
	var rowBuf [rowWidth]byte
	var lambdaSum float64
	for {
		rec, ok, e := cur.Next()
		if e != nil {
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		if !ok {
			break
		}

		s, e := arenaSeq.At(rec.URLRef)
		if e != nil {
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		ref, e := arena.intern(string(s))
		if e != nil {
			_ = arena.close()
			_ = recFile.Close()
			return res, e
		}
		rec.URLRef = ref
		// The outcome-string refs point into the base arena and are not re-interned here;
		// a change outcome re-establishes them, matching the compaction's carry rule.
		rec.ETagRef, rec.RedirectRef = 0, 0

		if rec.NextDue != 0 && rec.NextDue <= opts.NowHours {
			h := hostByKey[rec.HostKey]
			lambda, changed := foldOutcome(&rec, &h, drawOutcome(rec.URLKey, opts, rng), opts.NowHours, opts.Params, opts.Tau)
			lambdaSum += lambda
			res.Recrawled++
			if changed {
				res.Changed++
			} else {
				res.NoChange++
			}
		} else {
			res.Carried++
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

	// Phase 3: streaming columnar encode into a temp file, then an atomic rename.
	tmpOut := opts.OutPath + ".tmp"
	p := &format.Partition{
		ID:            opts.PartitionID,
		HostKeyLo:     hostKeyLo,
		HostKeyHi:     hostKeyHi,
		CreatedHours:  opts.NowHours,
		DefaultCodec:  opts.Codec,
		Hosts:         hostRecs,
		StringsAt:     arena.file(),
		StringsSize:   arena.size(),
		SeenFilter:    filter.Marshal(),
		MaxPageRows:   opts.PageRows,
		BlobFrontCode: true,
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
	res.URLCount = res.Recrawled + res.Carried
	res.HostCount = len(hostRecs)
	res.FileBytes = fi.Size()
	res.BitsPerURL = filter.BitsPerURL()
	if res.Recrawled > 0 {
		res.MeanLambda = lambdaSum / float64(res.Recrawled)
	}
	return res, nil
}

// drawOutcome produces one typed crawl outcome for a due row: with probability ChangeRate a
// real content change (a fresh fingerprint), otherwise a 304 no-change. The draw is off a
// seeded generator advanced once per due row, so a run is deterministic and reproducible.
func drawOutcome(key m.URLKey, opts RecrawlOptions, rng *rand.Rand) m.Outcome {
	o := m.Outcome{URLKey: key, FetchedAt: opts.NowHours, HTTPStatus: 200}
	if rng.Float64() < opts.ChangeRate {
		o.ContentFP = rng.Uint64() | 1 // nonzero: a body fingerprint the estimator can compare
		o.Simhash = rng.Uint64()
	} else {
		o.NotModified = true
		o.HTTPStatus = 304
	}
	return o
}

// foldOutcome advances a URL record for one crawl outcome and reschedules it, the freshness
// half of the frontier's markCrawled (doc 08 section 7.4, doc 06). It moves the row to
// Crawled, folds the body signal into the change-rate counters (a real change bumps the
// change count and clears the streak, a 304 or a cosmetic change extends the streak),
// re-estimates lambda, and sets the next due time from the water-filling allocation under
// the current water level. It returns the estimated lambda and whether the outcome was a
// real change. The OPIC cash spread and the soft-404 tombstone stay in the frontier layer;
// this is the per-URL freshness fold the live file needs to close the recrawl loop.
func foldOutcome(rec *m.URLRecord, h *m.HostRecord, o m.Outcome, now uint32, p freshness.Params, tau float64) (float64, bool) {
	rec.Status = m.StatusCrawled
	rec.RetryCount = 0
	if rec.FirstSeen == 0 {
		rec.FirstSeen = o.FetchedAt
	}
	rec.LastCrawled = o.FetchedAt
	rec.CrawlCount++

	changed := false
	if !o.NotModified && o.ContentFP != 0 {
		if rec.ContentFP != 0 {
			switch dedup.ClassifyChange(rec.ContentFP, o.ContentFP, rec.Simhash, o.Simhash) {
			case dedup.NoChange, dedup.CosmeticChange:
				rec.NoChangeStreak++
			case dedup.RealChange:
				rec.ChangeCount++
				rec.NoChangeStreak = 0
				rec.LastChanged = o.FetchedAt
				changed = true
			}
		} else {
			changed = true // first body signal, seeds the comparison for the next fetch
		}
		rec.ContentFP = o.ContentFP
		rec.Simhash = o.Simhash
	}
	if o.NotModified {
		rec.NoChangeStreak++
	}

	lambda := freshness.Estimate(rec, p)
	interval := freshness.TargetInterval(rec, h, tau, p)
	freshness.SetNextDue(rec, interval, now, p)
	return lambda, changed
}
