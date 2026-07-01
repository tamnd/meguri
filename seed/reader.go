package seed

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

// Reader reads a .mgs file. It mmaps the file so a block is a subslice of the
// mapping (zero copy for the raw codec) and its resident pages are reclaimable
// page cache, not heap, matching the .meguri read path. It is safe for concurrent
// Block calls only across distinct block indices sharing no decoder; a caller that
// fans blocks to workers gives each worker its own Reader or its own BlockReader.
type Reader struct {
	data   []byte
	unmap  func() error
	hdr    header
	blocks []blockMeta
}

// Open maps path and parses its header and footer index.
func Open(path string) (*Reader, error) {
	data, unmap, err := mmapFile(path)
	if err != nil {
		return nil, err
	}
	r, err := newReader(data, unmap)
	if err != nil {
		_ = unmap()
		return nil, err
	}
	return r, nil
}

// newReader parses the header and footer from mapped bytes, verifying the footer
// checksum. It is split from Open so tests can drive it off an in-memory buffer.
func newReader(data []byte, unmap func() error) (*Reader, error) {
	if len(data) < HeaderSize+trailerSize {
		return nil, ErrShortFile
	}
	hdr, err := decodeHeader(data[:HeaderSize])
	if err != nil {
		return nil, err
	}
	trailer := data[len(data)-trailerSize:]
	footerLen := int(binary.LittleEndian.Uint32(trailer[0:4]))
	footerCRC := binary.LittleEndian.Uint32(trailer[4:8])
	footerStart := len(data) - trailerSize - footerLen
	if footerStart < HeaderSize || uint64(footerStart) != hdr.footerOffset {
		return nil, ErrCorrupt
	}
	footer := data[footerStart : len(data)-trailerSize]
	if crc32.Checksum(footer, crcTable) != footerCRC {
		return nil, ErrChecksum
	}
	blocks, err := decodeFooter(footer, int(hdr.blockCount))
	if err != nil {
		return nil, err
	}
	return &Reader{data: data, unmap: unmap, hdr: hdr, blocks: blocks}, nil
}

// Close unmaps the file.
func (r *Reader) Close() error {
	if r.unmap != nil {
		return r.unmap()
	}
	return nil
}

// Blocks is the block count, the split granularity a driver hands to workers.
func (r *Reader) Blocks() int { return len(r.blocks) }

// RecordCount is the total number of URL records in the file.
func (r *Reader) RecordCount() uint64 { return r.hdr.recordCount }

// URLBytes is the total uncompressed URL byte count.
func (r *Reader) URLBytes() uint64 { return r.hdr.urlBytes }

// HostRange is the hostkey span this seed covers, from the header, the shard's
// range as seedpack recorded it.
func (r *Reader) HostRange() (lo, hi uint64) { return r.hdr.hostLo, r.hdr.hostHi }

// FirstKey returns block i's first URL bytes, the seek key. A caller binary-searches
// these to find where a lexical range begins.
func (r *Reader) FirstKey(i int) []byte { return r.blocks[i].firstKey }

// BlockReader returns a cursor over block i. For the raw codec the cursor reads
// straight from the mapping; for zstd it inflates the block into scratch once and
// reads from there. The returned URL bytes alias the mapping or the scratch and stay
// valid until the next Next call, so a caller that keeps a URL copies it.
func (r *Reader) BlockReader(i int) (*BlockReader, error) {
	if i < 0 || i >= len(r.blocks) {
		return nil, ErrCorrupt
	}
	m := r.blocks[i]
	var body []byte
	switch r.hdr.codec {
	case CodecZstd:
		start := int(m.offset)
		end := start + int(m.compLen)
		if start < HeaderSize || end > len(r.data) {
			return nil, ErrCorrupt
		}
		dec, err := getDecoder()
		if err != nil {
			return nil, err
		}
		raw, err := dec.DecodeAll(r.data[start:end], nil)
		if err != nil {
			return nil, err
		}
		body = raw
	default: // CodecRaw
		start := HeaderSize + i*int(r.hdr.blockSize)
		end := start + int(r.hdr.blockSize)
		if end > len(r.data) {
			return nil, ErrCorrupt
		}
		body = r.data[start:end]
	}
	return &BlockReader{body: body, remain: int(m.records)}, nil
}

// BlockReader is a cursor over one block's records.
type BlockReader struct {
	body   []byte
	pos    int
	remain int // records left, so padding after the last record is never misread
}

// Next returns the next URL bytes and true, or nil and false at the block end. The
// bytes alias the block body and are valid only until the next Next call.
func (br *BlockReader) Next() ([]byte, bool) {
	if br.remain <= 0 {
		return nil, false
	}
	n, k := binary.Uvarint(br.body[br.pos:])
	if k <= 0 {
		br.remain = 0
		return nil, false
	}
	br.pos += k
	if br.pos+int(n) > len(br.body) {
		br.remain = 0
		return nil, false
	}
	url := br.body[br.pos : br.pos+int(n)]
	br.pos += int(n)
	br.remain--
	return url, true
}

// getDecoder returns a shared zstd decoder. A single decoder is safe for concurrent
// DecodeAll, so one instance serves all readers.
var sharedDecoder *zstd.Decoder

func getDecoder() (*zstd.Decoder, error) {
	if sharedDecoder == nil {
		d, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		sharedDecoder = d
	}
	return sharedDecoder, nil
}
