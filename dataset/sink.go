package dataset

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// FileMeta describes one written .parquet file in the manifest.
type FileMeta struct {
	Name  string `json:"name"`  // path relative to the dataset root
	Rows  int64  `json:"rows"`  // row count
	Bytes int64  `json:"bytes"` // file size on disk
}

// rowSink is the export write target. write appends one row (buffered and flushed a
// row group at a time), close finalizes and stamps the byte count, abort discards a
// half-written output, and files reports what was written for the manifest.
type rowSink interface {
	write(row Row, st *ExportStats) error
	close(st *ExportStats) error
	abort() error
	files() []FileMeta
}

// pqFile is one open .parquet file: a GenericWriter over an *os.File, with a small
// row buffer handed to the writer in batches and an explicit row-group flush every
// rgRows so the resident cost stays one row group, not the whole table. It is the
// shared mechanism under both the single-file and the repo sinks.
type pqFile struct {
	path    string
	f       *os.File
	w       *parquet.GenericWriter[Row]
	rgRows  int
	sinceRG int
	batch   []Row
	rows    int64
}

// openPQ creates path and its writer. A nil codec leaves the file-level default
// compression unset, so only the columns whose schema tag names a codec (url, host,
// etag carry zstd) are compressed; a non-nil codec compresses every column.
func openPQ(path string, rgRows int, codec compress.Codec) (*pqFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	opts := []parquet.WriterOption{}
	if codec != nil {
		opts = append(opts, parquet.Compression(codec))
	}
	w := parquet.NewGenericWriter[Row](f, opts...)
	return &pqFile{path: path, f: f, w: w, rgRows: rgRows, batch: make([]Row, 0, writeBatch)}, nil
}

// add buffers a row, flushing the batch to the writer when it fills and ending the
// current row group when it reaches rgRows.
func (p *pqFile) add(row Row) error {
	p.batch = append(p.batch, row)
	if len(p.batch) >= writeBatch {
		if err := p.flushBatch(); err != nil {
			return err
		}
	}
	return nil
}

// flushBatch hands the buffered rows to the writer and closes a row group once rgRows
// have accumulated in it.
func (p *pqFile) flushBatch() error {
	if len(p.batch) == 0 {
		return nil
	}
	n, err := p.w.Write(p.batch)
	if err != nil {
		return err
	}
	p.rows += int64(n)
	p.sinceRG += n
	p.batch = p.batch[:0]
	if p.sinceRG >= p.rgRows {
		if err := p.w.Flush(); err != nil {
			return err
		}
		p.sinceRG = 0
	}
	return nil
}

// close flushes, finalizes the footer, and returns the row and byte counts.
func (p *pqFile) close() (rows, bytes int64, err error) {
	if err = p.flushBatch(); err != nil {
		return 0, 0, err
	}
	if err = p.w.Close(); err != nil {
		return 0, 0, err
	}
	if err = p.f.Close(); err != nil {
		return 0, 0, err
	}
	fi, err := os.Stat(p.path)
	if err != nil {
		return 0, 0, err
	}
	return p.rows, fi.Size(), nil
}

// abort closes and removes a half-written file.
func (p *pqFile) abort() error {
	_ = p.w.Close()
	_ = p.f.Close()
	return os.Remove(p.path)
}

// singleSink writes every row to one .parquet file.
type singleSink struct {
	pq   *pqFile
	name string
	meta []FileMeta
}

func newSingleSink(outFile string, rgRows int, codec compress.Codec) (*singleSink, error) {
	if dir := filepath.Dir(outFile); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	pq, err := openPQ(outFile, rgRows, codec)
	if err != nil {
		return nil, err
	}
	return &singleSink{pq: pq, name: filepath.Base(outFile)}, nil
}

func (s *singleSink) write(row Row, _ *ExportStats) error { return s.pq.add(row) }

func (s *singleSink) close(st *ExportStats) error {
	rows, bytes, err := s.pq.close()
	if err != nil {
		return err
	}
	st.Files = 1
	st.Bytes = bytes
	s.meta = []FileMeta{{Name: s.name, Rows: rows, Bytes: bytes}}
	return nil
}

func (s *singleSink) abort() error      { return s.pq.abort() }
func (s *singleSink) files() []FileMeta { return s.meta }

// repoSink writes a folder of evenly sized .parquet files, rotating to the next file
// when the current one reaches fileRows. The files land under dataDir named
// prefix-NNNNNN.parquet, the shape the Hugging Face viewer globs and a git-LFS push
// chunks cleanly.
type repoSink struct {
	dataDir  string
	relDir   string // dataDir relative to the dataset root, for manifest names
	prefix   string
	rgRows   int
	fileRows int
	codec    compress.Codec
	cur      *pqFile
	curRows  int64
	index    int
	meta     []FileMeta
}

func newRepoSink(dataDir, prefix string, startIndex, rgRows, fileRows int, codec compress.Codec) (*repoSink, error) {
	s := &repoSink{
		dataDir:  dataDir,
		relDir:   filepath.Base(dataDir),
		prefix:   prefix,
		rgRows:   rgRows,
		fileRows: fileRows,
		codec:    codec,
		index:    startIndex,
	}
	if err := s.rotate(); err != nil {
		return nil, err
	}
	return s, nil
}

// rotate closes the current file (recording its meta) and opens the next.
func (s *repoSink) rotate() error {
	if s.cur != nil {
		rows, bytes, err := s.cur.close()
		if err != nil {
			return err
		}
		s.meta = append(s.meta, FileMeta{
			Name:  filepath.Join(s.relDir, filepath.Base(s.cur.path)),
			Rows:  rows,
			Bytes: bytes,
		})
		s.index++
	}
	name := fmt.Sprintf("%s-%06d.parquet", s.prefix, s.index)
	pq, err := openPQ(filepath.Join(s.dataDir, name), s.rgRows, s.codec)
	if err != nil {
		return err
	}
	s.cur = pq
	s.curRows = 0
	return nil
}

func (s *repoSink) write(row Row, _ *ExportStats) error {
	if err := s.cur.add(row); err != nil {
		return err
	}
	s.curRows++
	if s.curRows >= int64(s.fileRows) {
		return s.rotate()
	}
	return nil
}

func (s *repoSink) close(st *ExportStats) error {
	rows, bytes, err := s.cur.close()
	if err != nil {
		return err
	}
	// A rotate that just fired leaves an empty current file; drop it from the manifest
	// so a dump on a file boundary does not publish a zero-row shard.
	if rows > 0 || len(s.meta) == 0 {
		s.meta = append(s.meta, FileMeta{
			Name:  filepath.Join(s.relDir, filepath.Base(s.cur.path)),
			Rows:  rows,
			Bytes: bytes,
		})
	} else {
		_ = os.Remove(s.cur.path)
	}
	s.cur = nil
	var total int64
	for _, m := range s.meta {
		total += m.Bytes
	}
	st.Files = len(s.meta)
	st.Bytes = total
	return nil
}

func (s *repoSink) abort() error {
	if s.cur != nil {
		return s.cur.abort()
	}
	return nil
}

func (s *repoSink) files() []FileMeta { return s.meta }
