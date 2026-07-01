package dataset

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/live"
)

// ExportOptions configures a meguri-to-Parquet export.
type ExportOptions struct {
	// RowGroupRows is the Parquet row-group size in rows. A row group is the unit a
	// reader seeks to and the unit column statistics summarize, so it trades scan
	// granularity against footer size. Zero uses a sane default.
	RowGroupRows int
	// FileRows caps the rows per output file in repo mode, so a 100M export becomes a
	// folder of evenly sized shards a git-LFS push and the Hugging Face viewer both
	// handle better than one multi-gigabyte blob. Zero uses a default; single-file
	// export ignores it.
	FileRows int
	// Codec is the column compression: "zstd" (default, best ratio), "snappy" (faster
	// decode), or "none".
	Codec string
	// SinceHours filters to rows with activity (first_seen, last_crawled, or
	// last_changed) at or after this epoch-hour, the incremental cursor. Zero exports
	// every row (a full dump). A caller sets it to a prior dump's watermark to export
	// only what is new or changed since.
	SinceHours uint32
}

// ExportStats reports what an export produced.
type ExportStats struct {
	Rows       int64  `json:"rows"`
	Skipped    int64  `json:"skipped"` // rows filtered out by SinceHours
	Hosts      int    `json:"hosts"`
	Files      int    `json:"files"`
	Bytes      int64  `json:"bytes"`
	LossyETags int64  `json:"lossy_etags"`
	Watermark  uint32 `json:"watermark_hours"` // max activity epoch-hour seen, the next incremental cursor
}

const (
	defaultRowGroupRows = 128 << 10 // 131072 rows per group
	defaultFileRows     = 4 << 20   // ~4.2M rows per file, a few hundred MB compressed
	writeBatch          = 4096      // rows handed to the writer per Write call
)

// compressionFor maps a codec name to a parquet compression codec.
func compressionFor(name string) (compress.Codec, error) {
	switch name {
	case "", "zstd":
		return &zstd.Codec{}, nil
	case "snappy":
		return &snappy.Codec{}, nil
	case "none", "raw", "uncompressed":
		return nil, nil
	default:
		return nil, fmt.Errorf("dataset: unknown codec %q (want zstd, snappy, or none)", name)
	}
}

// ExportSingle writes the whole source to one .parquet file. src is a .meguri file or
// a sharded store directory; a store's shards stream into the one file in shard order.
func ExportSingle(src, outFile string, opts ExportOptions) (ExportStats, error) {
	var st ExportStats
	codec, err := compressionFor(opts.Codec)
	if err != nil {
		return st, err
	}
	if opts.RowGroupRows <= 0 {
		opts.RowGroupRows = defaultRowGroupRows
	}
	sink, err := newSingleSink(outFile, opts.RowGroupRows, codec)
	if err != nil {
		return st, err
	}
	if err := exportSource(src, sink, opts.SinceHours, &st); err != nil {
		_ = sink.abort()
		return st, err
	}
	if err := sink.close(&st); err != nil {
		return st, err
	}
	return st, nil
}

// ExportRepo writes a Hugging Face dataset repo folder: data/urls-NNNNNN.parquet plus
// a manifest.json and a README.md dataset card. src is a .meguri file or a store dir.
// It is the publish-ready shape; each incremental dump adds files under data/ and is a
// commit on top.
func ExportRepo(src, outDir string, opts ExportOptions) (ExportStats, error) {
	var st ExportStats
	codec, err := compressionFor(opts.Codec)
	if err != nil {
		return st, err
	}
	if opts.RowGroupRows <= 0 {
		opts.RowGroupRows = defaultRowGroupRows
	}
	if opts.FileRows <= 0 {
		opts.FileRows = defaultFileRows
	}
	dataDir := filepath.Join(outDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return st, err
	}

	// A prior dump in this folder makes the export incremental: new files number after
	// the existing ones, and the manifest merges rather than replaces, so pushing the
	// result is a commit that grows the dataset. A first dump starts at index 0 with an
	// empty prior manifest.
	prev, hasPrev := Manifest{}, false
	if p, err := ReadManifest(outDir); err == nil {
		prev, hasPrev = p, true
	}
	sink, err := newRepoSink(dataDir, "urls", startIndex(prev), opts.RowGroupRows, opts.FileRows, codec)
	if err != nil {
		return st, err
	}
	if err := exportSource(src, sink, opts.SinceHours, &st); err != nil {
		_ = sink.abort()
		return st, err
	}
	if err := sink.close(&st); err != nil {
		return st, err
	}

	// The dataset self-describes: a manifest for tooling and a card for humans and
	// the Hugging Face viewer. Both are cheap and written last, after the data is on
	// disk, so an interrupted export leaves no half-written descriptor.
	man := newManifest(st, opts, sink.files())
	if hasPrev {
		man = mergeIncremental(prev, man)
	}
	if err := writeManifest(outDir, man); err != nil {
		return st, err
	}
	if err := writeCard(outDir, man); err != nil {
		return st, err
	}
	return st, nil
}

// startIndex is the next free data-file index for an incremental dump: one past the
// highest urls-NNNNNN.parquet the prior manifest lists, so new files never clobber
// published ones.
func startIndex(prev Manifest) int {
	next := 0
	for _, f := range prev.Files {
		base := filepath.Base(f.Name)
		var idx int
		if _, err := fmt.Sscanf(base, "urls-%06d.parquet", &idx); err == nil && idx >= next {
			next = idx + 1
		}
	}
	return next
}

// exportSource opens src (a file or a store dir) and streams every URL row into sink.
func exportSource(src string, sink rowSink, since uint32, st *ExportStats) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return exportStoreDir(src, sink, since, st)
	}
	return exportFile(src, sink, since, st)
}

// exportFile mmaps one .meguri and streams its URL table into sink.
func exportFile(path string, sink rowSink, since uint32, st *ExportStats) error {
	eng, err := live.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = eng.Close() }()
	return exportReader(eng.Reader(), sink, since, st)
}

// exportStoreDir opens a sharded store and streams every shard's URL table into sink
// in shard order, so the whole store lands in one contiguous, key-clustered dataset.
func exportStoreDir(dir string, sink rowSink, since uint32, st *ExportStats) error {
	s, err := live.OpenStore(dir)
	if err != nil {
		return fmt.Errorf("open store %s: %w", dir, err)
	}
	defer func() { _ = s.Close() }()
	for i := 0; i < s.Len(); i++ {
		if err := exportReader(s.Shard(i).Reader(), sink, since, st); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

// activityHours is the latest hour anything happened to a record: its discovery, its
// last crawl, or its last observed change. It is the incremental cursor's clock, so a
// row is "new or changed since h" exactly when its activity is at or after h. NextDue
// is deliberately excluded: it is a future time a scheduler sets, not an event.
func activityHours(rec *m.URLRecord) uint32 {
	return max(rec.FirstSeen, rec.LastCrawled, rec.LastChanged)
}

// arenaResolver resolves a string-arena ref to its bytes. Both format arena readers
// satisfy it: the sequential one is bounded to a page but demands ascending refs, the
// random one holds the whole arena but takes any order. The export picks between them
// by the file's blob layout.
type arenaResolver interface {
	At(ref uint64) ([]byte, error)
}

// newArenaResolvers returns the resolvers the export threads through one file, chosen by
// whether the blob is front-coded. A front-coded blob was written key-ordered (BulkLoad
// or a compaction), so the URL walk resolves it with ascending sequential readers whose
// transient is one page, the layout that scales to a 100M arena; hosts, urls, and etags
// each get their own forward reader since they advance independently. A non-front-coded
// blob is an engine checkpoint whose strings sit in discovery order, so a key-ordered
// walk jumps backward and one shared random reader (the whole arena resident) serves all
// three. seq reports which kind so the caller knows whether a backward etag ref is a
// real loss or cannot happen.
func newArenaResolvers(r *format.Reader) (host, url, etag arenaResolver, seq bool, err error) {
	if r.Header().Flags&format.FlagBlobFrontCoded != 0 {
		return r.ArenaSeqReader(), r.ArenaSeqReader(), r.ArenaSeqReader(), true, nil
	}
	rand, err := r.ArenaRandReader()
	if err != nil {
		return nil, nil, nil, false, err
	}
	return rand, rand, rand, false, nil
}

// exportReader is the core streaming pass: it walks one file's URL table in URLKey
// order, resolves each row's URL and ETag strings off the mapped arena, joins the host
// string by HostKey, and writes the Parquet row. For a front-coded file the transient
// is one column page (the row cursor) plus one arena page (the sequential reader), so a
// multi-gigabyte file exports in bounded memory; for an engine checkpoint the arena is
// held resident because its strings are not in key order.
func exportReader(r *format.Reader, sink rowSink, since uint32, st *ExportStats) error {
	hostAr, urlAr, etagAr, seq, err := newArenaResolvers(r)
	if err != nil {
		return err
	}
	hostByKey, err := resolveHosts(r, hostAr)
	if err != nil {
		return err
	}
	if len(hostByKey) > st.Hosts {
		st.Hosts = len(hostByKey)
	}

	cur, err := r.URLRows()
	if err != nil {
		return err
	}
	for {
		rec, ok, err := cur.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		act := activityHours(&rec)
		st.Watermark = max(st.Watermark, act)
		// A sequential URL reader must advance in ascending ref order, so a filtered
		// row's URL ref is still consumed to keep it monotonic; only the write is
		// skipped. A random reader does not care, but resolving anyway costs nothing.
		urlb, err := urlAr.At(rec.URLRef)
		if err != nil {
			return fmt.Errorf("resolve url ref %d: %w", rec.URLRef, err)
		}
		if since > 0 && act < since {
			st.Skipped++
			continue
		}
		etag := resolveETag(etagAr, rec.ETagRef, seq, st)
		row := ToRow(&rec, string(urlb), hostByKey[rec.HostKey], etag)
		if err := sink.write(row, st); err != nil {
			return err
		}
		st.Rows++
	}
	return nil
}

// resolveHosts builds the HostKey to host-string map for a file. The host table is
// small (far fewer hosts than URLs) and HostKey-sorted; a front-coded file interns the
// host strings at the low arena end in that same order so an ascending reader resolves
// them, and a random reader resolves them in any order, so either resolver works here.
func resolveHosts(r *format.Reader, ar arenaResolver) (map[uint64]string, error) {
	hosts, err := r.Hosts()
	if err != nil {
		return nil, err
	}
	mp := make(map[uint64]string, len(hosts))
	for i := range hosts {
		b, err := ar.At(hosts[i].HostRef)
		if err != nil {
			return nil, fmt.Errorf("resolve host ref %d: %w", hosts[i].HostRef, err)
		}
		mp[hosts[i].HostKey] = string(b)
	}
	return mp, nil
}

// resolveETag resolves an ETag arena ref best-effort. The live store zeroes ETagRef on
// every compaction and recrawl, so this is empty for a live-store export; a
// frontier-produced file may carry etags. With a sequential reader an etag interned out
// of ascending order cannot be seeked back to, so it is dropped to empty and counted, a
// reported loss rather than a corrupt read; a random reader has no such limit and
// resolves every etag. A corrupt span is likewise counted, never fatal.
func resolveETag(ar arenaResolver, ref uint64, seq bool, st *ExportStats) string {
	if ref == 0 {
		return ""
	}
	b, err := ar.At(ref)
	if err != nil {
		if seq && errors.Is(err, format.ErrArenaBackward) {
			st.LossyETags++
			return ""
		}
		st.LossyETags++
		return ""
	}
	return string(b)
}
