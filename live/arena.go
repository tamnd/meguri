package live

import (
	"bufio"
	"encoding/binary"
	"os"
)

// arenaWriter builds the string arena as a temp file: a leading zero sentinel at
// offset 0 (so a zero ref reads back as absent, matching format/blob.go) followed
// by uvarint-length-prefixed string spans, the fleet arena layout the .meguri blob
// region uses. The bulk loader writes host strings then URL strings here in key
// order, so the blob region the encoder frames from it is host-clustered and
// compresses well; the file is handed to StreamEncodeToFile as StringsAt and
// removed after the encode. It never lands the whole multi-gigabyte arena in RAM.
type arenaWriter struct {
	f   *os.File
	w   *bufio.Writer
	off uint64
	tmp []byte
}

// newArenaWriter creates the temp arena at path and writes the sentinel byte.
func newArenaWriter(path string) (*arenaWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	a := &arenaWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), tmp: make([]byte, binary.MaxVarintLen64)}
	if err := a.w.WriteByte(0); err != nil {
		_ = f.Close()
		return nil, err
	}
	a.off = 1
	return a, nil
}

// intern appends s and returns the offset its span starts at, the value a record's
// URLRef or a host's HostRef carries. An empty string interns to a real offset too;
// a caller that wants the absent sentinel passes a zero ref rather than "".
func (a *arenaWriter) intern(s string) (uint64, error) {
	off := a.off
	n := binary.PutUvarint(a.tmp, uint64(len(s)))
	if _, err := a.w.Write(a.tmp[:n]); err != nil {
		return 0, err
	}
	if _, err := a.w.WriteString(s); err != nil {
		return 0, err
	}
	a.off += uint64(n) + uint64(len(s))
	return off, nil
}

// size is the arena length so far, the StringsSize the encoder reads.
func (a *arenaWriter) size() int64 { return int64(a.off) }

// flush flushes the buffered writes so the file is complete for a reader. It does
// not close the file, which the caller keeps open as the encoder's StringsAt.
func (a *arenaWriter) flush() error { return a.w.Flush() }

// close flushes and closes the underlying file.
func (a *arenaWriter) close() error {
	if err := a.w.Flush(); err != nil {
		_ = a.f.Close()
		return err
	}
	return a.f.Close()
}

// file is the os.File the encoder reads through as StringsAt.
func (a *arenaWriter) file() *os.File { return a.f }
