package drum

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

// pending.go is the bucket-NNN.pending flush file (spec 2072 doc 04 section 3.3):
// a header then a sorted run of pending records. The CRC32C in the header is what
// makes a flush recoverable: a torn write leaves a file whose CRC does not match,
// and recovery discards it because the entries are reconstructable from the log
// (the discoveries that produced them are durable in the log before they enter a
// bucket, section 3.6). So a torn .pending file costs a re-derivation, never lost
// data.

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// pendingName is the on-disk name of a bucket's flush generation: bucket-NNN.SEQ.pending.
func pendingName(bucket int, seq uint32) string {
	return fmt.Sprintf("bucket-%03d.%d.pending", bucket, seq)
}

// writePendingFile sorts nothing (the caller flushes an already-sorted run) and
// writes entries to path with a CRC-checked header, then fsyncs. The write is one
// sequential append; nothing is sought.
func writePendingFile(path string, bucket int, entries []pendEntry) error {
	buf := make([]byte, pendHeaderSize+len(entries)*pendRecordSize)
	body := buf[pendHeaderSize:]
	for i, e := range entries {
		putPendRecord(body[i*pendRecordSize:], e)
	}
	crc := crc32.Checksum(body, crcTable)

	copy(buf[0:4], pendingMagic[:])
	buf[4] = pendVersion
	buf[5] = byte(bucket)
	// buf[6:8] reserved, left zero.
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(entries)))
	binary.LittleEndian.PutUint32(buf[12:16], crc)

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

// readPendingFile reads a whole pending file back into a sorted slice. ok is false
// when the header is malformed or the CRC does not match a torn or truncated file,
// the signal recovery uses to discard it (section 3.3).
func readPendingFile(path string) (entries []pendEntry, ok bool, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	if len(b) < pendHeaderSize {
		return nil, false, nil
	}
	if [4]byte{b[0], b[1], b[2], b[3]} != pendingMagic || b[4] != pendVersion {
		return nil, false, nil
	}
	count := binary.LittleEndian.Uint32(b[8:12])
	want := binary.LittleEndian.Uint32(b[12:16])
	body := b[pendHeaderSize:]
	if len(body) != int(count)*pendRecordSize {
		return nil, false, nil
	}
	if crc32.Checksum(body, crcTable) != want {
		return nil, false, nil
	}
	entries = make([]pendEntry, count)
	for i := range entries {
		entries[i] = getPendRecord(body[i*pendRecordSize:])
	}
	return entries, true, nil
}

// listPending returns every bucket-*.pending file under dir, sorted by name so a
// gather is deterministic (the merge order does not depend on it, but a stable
// list makes recovery reproducible).
func listPending(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range ents {
		n := e.Name()
		if len(n) > 8 && n[:7] == "bucket-" && filepath.Ext(n) == ".pending" {
			names = append(names, filepath.Join(dir, n))
		}
	}
	sort.Strings(names)
	return names, nil
}

// pendRun is a sorted reader over one pending file's entries, the merge's input
// run (doc 04 section 3.4 step 2). Entries are held in memory because a single
// flushed bucket is bounded by the flush threshold (~1 MiB), small enough to keep
// resident for the duration of one merge.
type pendRun struct {
	entries []pendEntry
	i       int
}

func newPendRun(entries []pendEntry) *pendRun { return &pendRun{entries: entries} }

func (r *pendRun) more() bool      { return r.i < len(r.entries) }
func (r *pendRun) peek() pendEntry { return r.entries[r.i] }
func (r *pendRun) take() pendEntry { e := r.entries[r.i]; r.i++; return e }
