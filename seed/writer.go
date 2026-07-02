package seed

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/klauspost/compress/zstd"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Writer builds one .seed file, appending URL records into fixed-size blocks and
// writing the header, block bodies, and footer index. It is single-goroutine, the
// one writer per shard seed. It streams: the resident cost is one block buffer plus
// the growing block index, not the corpus.
type Writer struct {
	f         *os.File
	blockSize int
	codec     Codec
	hostLo    uint64
	hostHi    uint64

	buf            []byte      // current block's record bytes, capacity blockSize
	firstKey       []byte      // first record's URL in the current block
	blocks         []blockMeta // footer index, one entry per flushed block
	offset         int64       // file offset where the next block body starts
	pendingRecords uint32      // records buffered in the current block, reset on flush

	records  uint64
	urlBytes uint64

	zw *zstd.Encoder // codec == CodecZstd only
}

// WriterOptions configure a Writer. BlockSize <= 0 uses DefaultBlockSize.
type WriterOptions struct {
	BlockSize int
	Codec     Codec
	HostLo    uint64
	HostHi    uint64
}

// NewWriter opens path for writing and reserves the header. The final header is
// written on Close once the counts and footer offset are known, which is why the
// file must be seekable.
func NewWriter(path string, opts WriterOptions) (*Writer, error) {
	bs := opts.BlockSize
	if bs <= 0 {
		bs = DefaultBlockSize
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// Reserve the header; the real bytes are written on Close.
	if _, err := f.Write(make([]byte, HeaderSize)); err != nil {
		_ = f.Close()
		return nil, err
	}
	w := &Writer{
		f:         f,
		blockSize: bs,
		codec:     opts.Codec,
		hostLo:    opts.HostLo,
		hostHi:    opts.HostHi,
		buf:       make([]byte, 0, bs),
		offset:    HeaderSize,
	}
	if w.codec == CodecZstd {
		zw, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		w.zw = zw
	}
	return w, nil
}

// SetHostRange records the hostkey span this shard covers, written into the header
// on Close. A producer that rolls shards at a host boundary knows a shard's HostHi
// only when it sees the next shard's first key, so it sets the range before closing.
func (w *Writer) SetHostRange(lo, hi uint64) { w.hostLo, w.hostHi = lo, hi }

// Records is the number of records added so far, so a rolling producer can decide
// when a shard has hit its target count.
func (w *Writer) Records() uint64 { return w.records }

// URLByteCount is the total URL bytes added so far.
func (w *Writer) URLByteCount() uint64 { return w.urlBytes }

// Add appends one URL record. A record is a uvarint length followed by the URL
// bytes; when the next record would not fit the current block, the block is flushed
// and a fresh one begins, so a record never straddles a block boundary.
func (w *Writer) Add(url []byte) error {
	frame := uvarintLen(uint64(len(url))) + len(url)
	if frame > w.blockSize {
		return ErrRecordTooBig
	}
	if len(w.buf)+frame > w.blockSize {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if len(w.buf) == 0 {
		w.firstKey = append(w.firstKey[:0], url...)
	}
	w.buf = binary.AppendUvarint(w.buf, uint64(len(url)))
	w.buf = append(w.buf, url...)
	w.pendingRecords++
	w.records++
	w.urlBytes += uint64(len(url))
	return nil
}

// AddString is Add for a string URL without forcing the caller to convert.
func (w *Writer) AddString(url string) error {
	return w.Add([]byte(url))
}

// flush writes the current block body and records its index entry. For the raw
// codec the body is the buffer padded to blockSize, so block offsets stay implicit;
// for zstd the body is the compressed buffer and the footer carries its offset and
// length.
func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	key := make([]byte, len(w.firstKey))
	copy(key, w.firstKey)
	m := blockMeta{
		offset:   uint64(w.offset),
		records:  w.pendingRecords,
		firstKey: key,
	}
	switch w.codec {
	case CodecZstd:
		comp := w.zw.EncodeAll(w.buf, nil)
		if _, err := w.f.Write(comp); err != nil {
			return err
		}
		m.compLen = uint32(len(comp))
		w.offset += int64(len(comp))
	default: // CodecRaw
		if _, err := w.f.Write(w.buf); err != nil {
			return err
		}
		if pad := w.blockSize - len(w.buf); pad > 0 {
			if _, err := w.f.Write(make([]byte, pad)); err != nil {
				return err
			}
		}
		m.compLen = uint32(w.blockSize)
		w.offset += int64(w.blockSize)
	}
	w.blocks = append(w.blocks, m)
	w.buf = w.buf[:0]
	w.firstKey = w.firstKey[:0]
	w.pendingRecords = 0
	return nil
}

// Close flushes the last block, writes the footer index and trailer, then rewrites
// the header with the final counts and footer offset.
func (w *Writer) Close() error {
	if err := w.flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	if w.zw != nil {
		_ = w.zw.Close()
	}
	footerOffset := w.offset
	footer := encodeFooter(w.blocks)
	if _, err := w.f.Write(footer); err != nil {
		_ = w.f.Close()
		return err
	}
	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(len(footer)))
	binary.LittleEndian.PutUint32(trailer[4:8], crc32.Checksum(footer, crcTable))
	if _, err := w.f.Write(trailer[:]); err != nil {
		_ = w.f.Close()
		return err
	}
	hdr := encodeHeader(header{
		codec:        w.codec,
		blockSize:    uint32(w.blockSize),
		recordCount:  w.records,
		urlBytes:     w.urlBytes,
		hostLo:       w.hostLo,
		hostHi:       w.hostHi,
		blockCount:   uint32(len(w.blocks)),
		footerOffset: uint64(footerOffset),
	})
	if _, err := w.f.WriteAt(hdr, 0); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
