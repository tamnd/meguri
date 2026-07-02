package drum

import (
	"encoding/binary"

	"github.com/tamnd/meguri"
)

// The on-disk byte layouts of the DRUM (spec 2072 doc 04 sections 3.2-3.3). All
// integers are little-endian, the fleet byte order matching store/codec.go. The
// repository record is fixed width so the repository is binary-searchable (the
// point-read path, doc 04 section 4.2) and the merge is a pure linear sweep with
// no length prefixes to decode.

const (
	// repoRecordSize is the fixed width of one (URLKey, locEntry) repository
	// record: HostKey(8) PathKey(8) off(8) lsn_lo(4) flags(1).
	repoRecordSize = 29

	// pendRecordSize is one pending record: a repository record plus the op byte.
	// A pending entry is a check-insert (discovery) or an insert-only (recovery);
	// the repository never stores op because every key in it is already inserted.
	pendRecordSize = 30

	// pendHeaderSize is the pending-file header: magic, version, bucket, count,
	// crc32c of the records that follow (doc 04 section 3.3).
	pendHeaderSize = 16

	// idxEntrySize is one sparse-block-index entry: the first key of a repository
	// block and the block's byte offset (doc 04 section 4.2).
	idxEntrySize = 24

	// blockRecords is the number of repository records per block the sparse index
	// points at. 141 records is 4089 bytes, the ~4 KiB block the spec sizes so the
	// resident index is ~0.17 B/url, a rounding error against the per-URL
	// structures the redesign kills.
	blockRecords = 141

	// flags bit positions in a repository or pending record.
	flagTomb = 1 << 0
)

// pendingMagic marks a bucket-NNN.pending file: "MGDP", meguri drum pending.
var pendingMagic = [4]byte{'M', 'G', 'D', 'P'}

// pendVersion is the current pending-file layout version.
const pendVersion = 1

// locEntry is the DRUM aux value: the on-disk form of the store's urlLoc with the
// resident *rec pointer removed (doc 04 section 2.3). It is what the repository
// stores next to each URLKey, so a membership probe and a location lookup are the
// same probe (D2). The body is paged from the log at off; the resident pointer is
// gone, which is the ~81 B/url resident term the redesign removes.
type locEntry struct {
	off  int64  // log offset of the current record frame, the urlLoc.off
	lsn  uint64 // version, the urlLoc.lsn, for last-writer-wins on merge
	tomb bool   // tombstone bit, the urlLoc.tomb
}

// op distinguishes a discovery check-and-insert from a recovery insert-only.
type op uint8

const (
	opCheckInsert op = 0 // discovery: classify unique-or-duplicate, then insert
	opInsertOnly  op = 1 // recovery/handoff: insert without a verdict
)

// repoEntry is one decoded repository record: the key and its location.
type repoEntry struct {
	key meguri.URLKey
	loc locEntry
}

// pendEntry is one decoded pending record: a repoEntry plus its op.
type pendEntry struct {
	key meguri.URLKey
	loc locEntry
	op  op
}

// putRepoRecord writes a 29-byte repository record into b (which must be at least
// repoRecordSize long).
func putRepoRecord(b []byte, key meguri.URLKey, loc locEntry) {
	binary.LittleEndian.PutUint64(b[0:8], key.HostKey)
	binary.LittleEndian.PutUint64(b[8:16], key.PathKey)
	binary.LittleEndian.PutUint64(b[16:24], uint64(loc.off))
	binary.LittleEndian.PutUint32(b[24:28], uint32(loc.lsn))
	var flags byte
	if loc.tomb {
		flags |= flagTomb
	}
	b[28] = flags
}

// getRepoRecord decodes a 29-byte repository record from b.
func getRepoRecord(b []byte) repoEntry {
	var e repoEntry
	e.key.HostKey = binary.LittleEndian.Uint64(b[0:8])
	e.key.PathKey = binary.LittleEndian.Uint64(b[8:16])
	e.loc.off = int64(binary.LittleEndian.Uint64(b[16:24]))
	e.loc.lsn = uint64(binary.LittleEndian.Uint32(b[24:28]))
	e.loc.tomb = b[28]&flagTomb != 0
	return e
}

// putPendRecord writes a 30-byte pending record into b.
func putPendRecord(b []byte, e pendEntry) {
	putRepoRecord(b, e.key, e.loc)
	b[29] = byte(e.op)
}

// getPendRecord decodes a 30-byte pending record from b.
func getPendRecord(b []byte) pendEntry {
	re := getRepoRecord(b)
	return pendEntry{key: re.key, loc: re.loc, op: op(b[29])}
}
