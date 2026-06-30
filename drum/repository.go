package drum

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/tamnd/meguri"
)

// repository.go is the single sorted (URLKey, locEntry) file that holds every key
// the partition has ever seen (spec 2072 doc 04 sections 3.1-3.2) and the sparse
// block index over it that serves the point read (section 4.2). The repository is
// rewritten, never mutated: a merge reads the old one sequentially and writes a
// new one sequentially, so every byte of the merge is sequential disk bandwidth,
// not random IOPS (section 3.5).

const (
	repoName     = "repository"
	repoNextName = "repository.next"
	repoIdxName  = "repository.idx"
)

// repoReader is a buffered sequential reader over the sorted repository, the
// merge's repository input and the recovery scan. It yields one repoEntry at a
// time in URLKey order, never materializing the file as a slice.
type repoReader struct {
	f   *os.File
	buf []byte
	n   int // valid bytes in buf
	o   int // read cursor within buf
}

// openRepoReader opens the repository for a sequential sweep. A missing repository
// (the partition has never merged) is an empty reader, not an error.
func openRepoReader(path string, bufBytes int) (*repoReader, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return &repoReader{}, nil
	}
	if err != nil {
		return nil, err
	}
	if bufBytes < repoRecordSize {
		bufBytes = 64 << 10
	}
	// Buffer a whole number of records so a record never straddles a refill.
	bufBytes -= bufBytes % repoRecordSize
	return &repoReader{f: f, buf: make([]byte, bufBytes)}, nil
}

// next returns the next repository record in key order. ok is false at end of file.
func (r *repoReader) next() (repoEntry, bool, error) {
	if r.f == nil {
		return repoEntry{}, false, nil
	}
	if r.o+repoRecordSize > r.n {
		if err := r.refill(); err != nil {
			return repoEntry{}, false, err
		}
		if r.n < repoRecordSize {
			return repoEntry{}, false, nil
		}
	}
	e := getRepoRecord(r.buf[r.o:])
	r.o += repoRecordSize
	return e, true, nil
}

// refill carries the partial-record tail to the front and reads more.
func (r *repoReader) refill() error {
	rem := r.n - r.o
	copy(r.buf, r.buf[r.o:r.n])
	r.o = 0
	r.n = rem
	for r.n+repoRecordSize <= len(r.buf) {
		m, err := r.f.Read(r.buf[r.n:])
		r.n += m
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if m == 0 {
			break
		}
	}
	return nil
}

func (r *repoReader) close() error {
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}

// repoWriter writes a fresh repository sequentially to repository.next, building
// the sparse block index as it goes, then fsyncs and atomically renames it over
// the live repository (doc 04 section 3.4 steps 4-5). A crash mid-merge leaves
// either the old repository or the new one, never a half-written one.
type repoWriter struct {
	f        *os.File
	nextPath string
	livePath string
	idxPath  string
	bw       []byte // record-aligned write buffer
	written  int64  // bytes written to the file so far
	count    int64  // records written
	idx      []idxEntry
}

// idxEntry is one resident sparse-index entry: the first key of a repository block
// and the block's byte offset (doc 04 section 4.2).
type idxEntry struct {
	key      meguri.URLKey
	blockOff int64
}

func newRepoWriter(nextPath, livePath, idxPath string, bufBytes int) (*repoWriter, error) {
	f, err := os.OpenFile(nextPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if bufBytes < repoRecordSize {
		bufBytes = 64 << 10
	}
	bufBytes -= bufBytes % repoRecordSize
	return &repoWriter{
		f:        f,
		nextPath: nextPath,
		livePath: livePath,
		idxPath:  idxPath,
		bw:       make([]byte, 0, bufBytes),
	}, nil
}

// write appends one record in key order and records a block-index entry at every
// blockRecords-th record (the first key of each block).
func (w *repoWriter) write(key meguri.URLKey, loc locEntry) error {
	if w.count%blockRecords == 0 {
		w.idx = append(w.idx, idxEntry{key: key, blockOff: w.written + int64(len(w.bw))})
	}
	var rec [repoRecordSize]byte
	putRepoRecord(rec[:], key, loc)
	w.bw = append(w.bw, rec[:]...)
	w.count++
	if len(w.bw) >= cap(w.bw) {
		return w.flush()
	}
	return nil
}

func (w *repoWriter) flush() error {
	if len(w.bw) == 0 {
		return nil
	}
	if _, err := w.f.Write(w.bw); err != nil {
		return err
	}
	w.written += int64(len(w.bw))
	w.bw = w.bw[:0]
	return nil
}

// finishAndSync flushes the tail, writes the block index file, and fsyncs both.
func (w *repoWriter) finishAndSync() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	return writeIndexFile(w.idxPath, w.idx)
}

// renameOver atomically swaps repository.next over the live repository.
func (w *repoWriter) renameOver() error {
	return os.Rename(w.nextPath, w.livePath)
}

// writeIndexFile writes the sparse block index, fsynced. It is rebuildable from
// the repository, so a torn index is reconstructed at Open, but writing it durably
// keeps the common-case Open from rescanning the whole repository.
func writeIndexFile(path string, idx []idxEntry) error {
	buf := make([]byte, len(idx)*idxEntrySize)
	for i, e := range idx {
		binary.LittleEndian.PutUint64(buf[i*idxEntrySize+0:], e.key.HostKey)
		binary.LittleEndian.PutUint64(buf[i*idxEntrySize+8:], e.key.PathKey)
		binary.LittleEndian.PutUint64(buf[i*idxEntrySize+16:], uint64(e.blockOff))
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// blockIndex is the resident sparse index: the first key of every block, sorted,
// with the block's byte offset. A point read binary-searches it (no disk) to find
// the one block whose key range covers the target, then reads that block.
type blockIndex struct {
	entries []idxEntry
}

// loadBlockIndex reads repository.idx into RAM, or rebuilds it from the repository
// if the index file is missing or torn (a crash between the repository rename and
// the index write). A missing repository yields an empty index.
func loadBlockIndex(idxPath, repoPath string) (*blockIndex, error) {
	b, err := os.ReadFile(idxPath)
	if err == nil && len(b)%idxEntrySize == 0 && len(b) > 0 {
		entries := make([]idxEntry, len(b)/idxEntrySize)
		for i := range entries {
			entries[i].key.HostKey = binary.LittleEndian.Uint64(b[i*idxEntrySize+0:])
			entries[i].key.PathKey = binary.LittleEndian.Uint64(b[i*idxEntrySize+8:])
			entries[i].blockOff = int64(binary.LittleEndian.Uint64(b[i*idxEntrySize+16:]))
		}
		return &blockIndex{entries: entries}, nil
	}
	return rebuildBlockIndex(repoPath)
}

// rebuildBlockIndex scans the repository and re-derives the sparse index, the
// recovery path when the index file is absent or torn.
func rebuildBlockIndex(repoPath string) (*blockIndex, error) {
	rr, err := openRepoReader(repoPath, 1<<20)
	if err != nil {
		return nil, err
	}
	defer rr.close()
	var idx []idxEntry
	var n int64
	off := int64(0)
	for {
		e, ok, err := rr.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if n%blockRecords == 0 {
			idx = append(idx, idxEntry{key: e.key, blockOff: off})
		}
		n++
		off += repoRecordSize
	}
	return &blockIndex{entries: idx}, nil
}

// blockFor returns the byte offset of the block whose key range covers key: the
// last index entry whose first key is <= key. Returns -1 when key is below the
// first block (the key cannot be in the repository).
func (bi *blockIndex) blockFor(key meguri.URLKey) int64 {
	if len(bi.entries) == 0 || key.Less(bi.entries[0].key) {
		return -1
	}
	// Largest i with entries[i].key <= key.
	i := sort.Search(len(bi.entries), func(i int) bool { return key.Less(bi.entries[i].key) })
	return bi.entries[i-1].blockOff
}
