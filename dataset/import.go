package dataset

import (
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/live"
)

// ImportOptions configures a Parquet-to-meguri import.
type ImportOptions struct {
	// TmpDir is the scratch directory for the build's arena and record temps. Empty
	// uses the system default.
	TmpDir string
	// Codec is the output .meguri body codec: format.CodecZstd (default) or
	// format.CodecNone. It is the meguri codec, not the Parquet one.
	Codec uint8
	// PageRows caps the output column page size. Zero uses a scale-friendly default so
	// a dedup confirm decodes one page, not the whole column.
	PageRows int
	// FPRate is the resident seen-set filter false-positive budget. Zero uses the
	// build default.
	FPRate float64
	// ExpectedKeys sizes the filter. Zero lets the build guess; a caller that knows the
	// row count (the manifest does) should set it so the filter is not oversized.
	ExpectedKeys uint64
	// PartitionID stamps the output partition.
	PartitionID uint32
}

const defaultImportPageRows = 65536

// Import builds one .meguri file from a Parquet dataset. in is a dataset repo folder
// (with a manifest.json listing the data files) or a single .parquet file; out is the
// .meguri to write. The rows are read back in URLKey order across all files, with a
// later file winning a key tie, so an incremental dataset (where a changed URL was
// re-exported in a newer file) imports to the latest state of each URL with no
// duplicates.
func Import(in, out string, opts ImportOptions) (live.BuildResult, error) {
	files, expected, err := resolveInput(in)
	if err != nil {
		return live.BuildResult{}, err
	}
	if opts.ExpectedKeys == 0 {
		opts.ExpectedKeys = expected
	}
	pageRows := opts.PageRows
	if pageRows <= 0 {
		pageRows = defaultImportPageRows
	}

	src, err := newMergeSource(files)
	if err != nil {
		return live.BuildResult{}, err
	}
	defer src.Close()

	bo := live.BuildOptions{
		Path:         out,
		TmpDir:       opts.TmpDir,
		ExpectedKeys: opts.ExpectedKeys,
		PageRows:     pageRows,
		Codec:        opts.Codec,
		FPRate:       opts.FPRate,
		PartitionID:  opts.PartitionID,
	}
	res, err := live.ImportRecords(src, bo)
	if err != nil {
		return res, err
	}
	if src.err != nil {
		return res, src.err
	}
	return res, nil
}

// resolveInput turns the in argument into the ordered list of .parquet files to read
// and the total row count if a manifest is present. A directory is read as a dataset
// repo (its manifest names the files, in append order so later dumps sort last); a
// file is read as a single .parquet.
func resolveInput(in string) (files []string, expected uint64, err error) {
	info, err := os.Stat(in)
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return []string{in}, 0, nil
	}
	man, err := ReadManifest(in)
	if err != nil {
		return nil, 0, fmt.Errorf("read manifest in %s: %w", in, err)
	}
	if len(man.Files) == 0 {
		return nil, 0, fmt.Errorf("dataset %s lists no data files", in)
	}
	files = make([]string, len(man.Files))
	for i, f := range man.Files {
		files[i] = filepath.Join(in, f.Name)
	}
	return files, uint64(man.Rows), nil
}

// fileCursor streams one .parquet file's rows in order, buffering a batch at a time so
// the resident cost is one batch, not the file.
type fileCursor struct {
	rank int // position in the ordered file list; a higher rank wins a key tie
	f    *os.File
	r    *parquet.GenericReader[Row]
	buf  []Row
	pos  int
	done bool
	err  error
}

const readBatch = 4096

func openCursor(path string, rank int) (*fileCursor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	c := &fileCursor{
		rank: rank,
		f:    f,
		r:    parquet.NewGenericReader[Row](f),
		buf:  make([]Row, readBatch),
	}
	c.fill()
	return c, c.err
}

// fill reads the next batch. It sets done at the end of the file.
func (c *fileCursor) fill() {
	n, err := c.r.Read(c.buf)
	c.buf = c.buf[:n]
	c.pos = 0
	if err != nil && err != io.EOF {
		c.err = err
	}
	if n == 0 {
		c.done = true
	}
}

// head returns the current row without advancing. ok is false at end of file.
func (c *fileCursor) head() (Row, bool) {
	if c.done || c.pos >= len(c.buf) {
		return Row{}, false
	}
	return c.buf[c.pos], true
}

// advance moves past the current row, refilling the buffer when it drains.
func (c *fileCursor) advance() {
	c.pos++
	if c.pos >= len(c.buf) && !c.done {
		c.fill()
	}
}

func (c *fileCursor) Close() error {
	_ = c.r.Close()
	return c.f.Close()
}

// mergeSource is the RecordSource live.ImportRecords consumes: a k-way merge over the
// file cursors that yields rows in global URLKey order and drops all but the newest
// copy of a duplicated key. Files within one dump are non-overlapping and ordered, so
// with a single dump the merge degenerates to a concatenation; across dumps it dedups.
type mergeSource struct {
	h   cursorHeap
	err error
}

func newMergeSource(files []string) (*mergeSource, error) {
	ms := &mergeSource{}
	for i, path := range files {
		c, err := openCursor(path, i)
		if err != nil {
			for _, oc := range ms.h {
				_ = oc.Close()
			}
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		if _, ok := c.head(); ok {
			ms.h = append(ms.h, c)
		} else {
			_ = c.Close()
		}
	}
	heap.Init(&ms.h)
	return ms, nil
}

// Next pops the smallest key across all cursors, resolving a tie in favor of the
// highest-rank (newest) cursor, and advances every cursor that held that key so a
// duplicate is consumed rather than re-emitted.
func (ms *mergeSource) Next() (live.RecordItem, bool, error) {
	if ms.err != nil {
		return live.RecordItem{}, false, ms.err
	}
	if len(ms.h) == 0 {
		return live.RecordItem{}, false, nil
	}
	top, _ := ms.h[0].head()
	key := m.URLKey{HostKey: top.HostKey, PathKey: top.PathKey}

	best, bestRank := top, ms.h[0].rank
	// Drain every cursor whose head equals key, keeping the newest row.
	for len(ms.h) > 0 {
		row, ok := ms.h[0].head()
		if !ok {
			heap.Pop(&ms.h)
			continue
		}
		if (m.URLKey{HostKey: row.HostKey, PathKey: row.PathKey}) != key {
			break
		}
		if ms.h[0].rank >= bestRank {
			best, bestRank = row, ms.h[0].rank
		}
		c := ms.h[0]
		if c.err != nil {
			ms.err = c.err
			return live.RecordItem{}, false, ms.err
		}
		c.advance()
		if _, ok := c.head(); ok {
			heap.Fix(&ms.h, 0)
		} else {
			heap.Pop(&ms.h)
		}
	}

	rec, url, host, etag := FromRow(&best)
	return live.RecordItem{Rec: rec, URL: url, Host: host, ETag: etag}, true, nil
}

func (ms *mergeSource) Close() {
	for _, c := range ms.h {
		_ = c.Close()
	}
}

// cursorHeap is a min-heap of file cursors ordered by head key, so the smallest
// outstanding URLKey is always at the root.
type cursorHeap []*fileCursor

func (h cursorHeap) Len() int { return len(h) }

func (h cursorHeap) Less(i, j int) bool {
	a, _ := h[i].head()
	b, _ := h[j].head()
	ak := m.URLKey{HostKey: a.HostKey, PathKey: a.PathKey}
	bk := m.URLKey{HostKey: b.HostKey, PathKey: b.PathKey}
	if ak != bk {
		return ak.Less(bk)
	}
	// Equal keys: order the newer (higher-rank) cursor first so a tie resolves to it
	// even though Next inspects every equal-key cursor anyway.
	return h[i].rank > h[j].rank
}

func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *cursorHeap) Push(x any) { *h = append(*h, x.(*fileCursor)) }

func (h *cursorHeap) Pop() any {
	old := *h
	n := len(old)
	c := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return c
}
