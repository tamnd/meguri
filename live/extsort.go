package live

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	m "github.com/tamnd/meguri"
)

// extsort is the bounded-memory external sort the bulk loader runs over the corpus.
// It buffers discovered (key, url) pairs up to a row cap, sorts each buffer by
// URLKey, and spills it as a sorted run file; a k-way merge then yields the pairs
// in global URLKey order, deduplicating equal adjacent keys. Memory is one buffer
// plus one read-ahead record per run, never the whole corpus, which is what lets a
// 100M build run on the 5 GiB box.

// sortItem is one discovered URL on its way into the sort: its key and its
// canonical string. The string is held only until the run it lands in is spilled.
type sortItem struct {
	key m.URLKey
	url string
}

// runBuilder accumulates items, sorting and spilling a run each time the buffer
// fills. The run files are written under dir as run-NNNNN.
type runBuilder struct {
	dir     string
	cap     int
	buf     []sortItem
	runs    []string
	scratch []byte
}

func newRunBuilder(dir string, rowCap int) *runBuilder {
	if rowCap <= 0 {
		rowCap = 1 << 20
	}
	return &runBuilder{
		dir:     dir,
		cap:     rowCap,
		buf:     make([]sortItem, 0, rowCap),
		scratch: make([]byte, binary.MaxVarintLen64),
	}
}

// add appends one item, spilling a sorted run when the buffer reaches the cap.
func (rb *runBuilder) add(key m.URLKey, url string) error {
	rb.buf = append(rb.buf, sortItem{key: key, url: url})
	if len(rb.buf) >= rb.cap {
		return rb.spill()
	}
	return nil
}

// spill sorts the buffer by URLKey and writes it as the next run file.
func (rb *runBuilder) spill() error {
	if len(rb.buf) == 0 {
		return nil
	}
	sort.Slice(rb.buf, func(i, j int) bool { return rb.buf[i].key.Less(rb.buf[j].key) })
	path := filepath.Join(rb.dir, fmt.Sprintf("run-%05d", len(rb.runs)))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	for i := range rb.buf {
		if err := writeRunItem(w, rb.scratch, &rb.buf[i]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	rb.runs = append(rb.runs, path)
	rb.buf = rb.buf[:0]
	return nil
}

// finish spills any remaining buffered items and returns the run file paths.
func (rb *runBuilder) finish() ([]string, error) {
	if err := rb.spill(); err != nil {
		return nil, err
	}
	return rb.runs, nil
}

func writeRunItem(w *bufio.Writer, scratch []byte, it *sortItem) error {
	var key [16]byte
	binary.LittleEndian.PutUint64(key[0:], it.key.HostKey)
	binary.LittleEndian.PutUint64(key[8:], it.key.PathKey)
	if _, err := w.Write(key[:]); err != nil {
		return err
	}
	n := binary.PutUvarint(scratch, uint64(len(it.url)))
	if _, err := w.Write(scratch[:n]); err != nil {
		return err
	}
	_, err := w.WriteString(it.url)
	return err
}

// runReader yields the items of one run file in order.
type runReader struct {
	f    *os.File
	r    *bufio.Reader
	cur  sortItem
	done bool
}

func openRunReader(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{f: f, r: bufio.NewReaderSize(f, 1<<20)}
	if err := rr.advance(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return rr, nil
}

func (rr *runReader) advance() error {
	var key [16]byte
	if _, err := io.ReadFull(rr.r, key[:]); err != nil {
		if err == io.EOF {
			rr.done = true
			return nil
		}
		return err
	}
	n, err := binary.ReadUvarint(rr.r)
	if err != nil {
		return err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(rr.r, buf); err != nil {
		return err
	}
	rr.cur = sortItem{
		key: m.URLKey{
			HostKey: binary.LittleEndian.Uint64(key[0:]),
			PathKey: binary.LittleEndian.Uint64(key[8:]),
		},
		url: string(buf),
	}
	return nil
}

func (rr *runReader) close() error { return rr.f.Close() }

// runHeap is the min-heap of run readers keyed by their current item.
type runHeap []*runReader

func (h runHeap) Len() int            { return len(h) }
func (h runHeap) Less(i, j int) bool  { return h[i].cur.key.Less(h[j].cur.key) }
func (h runHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)         { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// mergeRuns opens every run file and returns a function that yields items in
// global URLKey order, deduplicating equal adjacent keys (the first wins). The
// returned next returns ok false when the runs are exhausted, and the returned
// close shuts every run reader.
func mergeRuns(paths []string) (next func() (sortItem, bool, error), closeAll func() error, err error) {
	h := &runHeap{}
	readers := make([]*runReader, 0, len(paths))
	for _, p := range paths {
		rr, e := openRunReader(p)
		if e != nil {
			for _, r := range readers {
				_ = r.close()
			}
			return nil, nil, e
		}
		readers = append(readers, rr)
		if !rr.done {
			heap.Push(h, rr)
		}
	}
	var have bool
	var last m.URLKey
	next = func() (sortItem, bool, error) {
		for h.Len() > 0 {
			top := (*h)[0]
			it := top.cur
			if e := top.advance(); e != nil {
				return sortItem{}, false, e
			}
			if top.done {
				heap.Pop(h)
			} else {
				heap.Fix(h, 0)
			}
			if have && it.key == last {
				continue // drop a duplicate key, keeping the first seen
			}
			have, last = true, it.key
			return it, true, nil
		}
		return sortItem{}, false, nil
	}
	closeAll = func() error {
		var firstErr error
		for _, r := range readers {
			if e := r.close(); e != nil && firstErr == nil {
				firstErr = e
			}
		}
		return firstErr
	}
	return next, closeAll, nil
}
