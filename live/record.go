package live

import (
	"encoding/binary"
	"math"

	m "github.com/tamnd/meguri"
)

// rowWidth is the fixed serialized length of a URLRecord in the key-ordered temp
// file the bulk loader feeds to the encoder. Every field is fixed-width, including
// the 16-byte URLKey, so the records temp is read back as a flat array of rows with
// no length framing. HostKey is not stored: it is the high half of the key and is
// refilled on decode.
const rowWidth = 16 + // URLKey (HostKey, PathKey)
	1 + 4 + 2 + 1 + // Status, Priority, Depth, DiscoverySource
	8 + // URLRef
	4 + 4 + 4 + 4 + // FirstSeen, LastCrawled, LastChanged, NextDue
	4 + 4 + 4 + 2 + // Lambda, CrawlCount, ChangeCount, NoChangeStreak
	8 + 4 + // ETagRef, LastModified
	8 + 8 + // ContentFP, Simhash
	2 + 8 + // HTTPStatus, RedirectRef
	1 + 2 // RetryCount, ErrorCount

// encodeRow writes r into dst[:rowWidth] in little-endian field order.
func encodeRow(dst []byte, r *m.URLRecord) {
	_ = dst[rowWidth-1]
	binary.LittleEndian.PutUint64(dst[0:], r.URLKey.HostKey)
	binary.LittleEndian.PutUint64(dst[8:], r.URLKey.PathKey)
	i := 16
	dst[i] = uint8(r.Status)
	i++
	binary.LittleEndian.PutUint32(dst[i:], math.Float32bits(r.Priority))
	i += 4
	binary.LittleEndian.PutUint16(dst[i:], r.Depth)
	i += 2
	dst[i] = uint8(r.DiscoverySource)
	i++
	binary.LittleEndian.PutUint64(dst[i:], r.URLRef)
	i += 8
	binary.LittleEndian.PutUint32(dst[i:], r.FirstSeen)
	i += 4
	binary.LittleEndian.PutUint32(dst[i:], r.LastCrawled)
	i += 4
	binary.LittleEndian.PutUint32(dst[i:], r.LastChanged)
	i += 4
	binary.LittleEndian.PutUint32(dst[i:], r.NextDue)
	i += 4
	binary.LittleEndian.PutUint32(dst[i:], math.Float32bits(r.Lambda))
	i += 4
	binary.LittleEndian.PutUint32(dst[i:], r.CrawlCount)
	i += 4
	binary.LittleEndian.PutUint32(dst[i:], r.ChangeCount)
	i += 4
	binary.LittleEndian.PutUint16(dst[i:], r.NoChangeStreak)
	i += 2
	binary.LittleEndian.PutUint64(dst[i:], r.ETagRef)
	i += 8
	binary.LittleEndian.PutUint32(dst[i:], r.LastModified)
	i += 4
	binary.LittleEndian.PutUint64(dst[i:], r.ContentFP)
	i += 8
	binary.LittleEndian.PutUint64(dst[i:], r.Simhash)
	i += 8
	binary.LittleEndian.PutUint16(dst[i:], r.HTTPStatus)
	i += 2
	binary.LittleEndian.PutUint64(dst[i:], r.RedirectRef)
	i += 8
	dst[i] = r.RetryCount
	i++
	binary.LittleEndian.PutUint16(dst[i:], r.ErrorCount)
}

// decodeRow reverses encodeRow, refilling HostKey from the key's high half.
func decodeRow(b []byte) m.URLRecord {
	_ = b[rowWidth-1]
	var r m.URLRecord
	r.URLKey.HostKey = binary.LittleEndian.Uint64(b[0:])
	r.URLKey.PathKey = binary.LittleEndian.Uint64(b[8:])
	r.HostKey = r.URLKey.HostKey
	i := 16
	r.Status = m.URLStatus(b[i])
	i++
	r.Priority = math.Float32frombits(binary.LittleEndian.Uint32(b[i:]))
	i += 4
	r.Depth = binary.LittleEndian.Uint16(b[i:])
	i += 2
	r.DiscoverySource = m.DiscoverySource(b[i])
	i++
	r.URLRef = binary.LittleEndian.Uint64(b[i:])
	i += 8
	r.FirstSeen = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.LastCrawled = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.LastChanged = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.NextDue = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.Lambda = math.Float32frombits(binary.LittleEndian.Uint32(b[i:]))
	i += 4
	r.CrawlCount = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.ChangeCount = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.NoChangeStreak = binary.LittleEndian.Uint16(b[i:])
	i += 2
	r.ETagRef = binary.LittleEndian.Uint64(b[i:])
	i += 8
	r.LastModified = binary.LittleEndian.Uint32(b[i:])
	i += 4
	r.ContentFP = binary.LittleEndian.Uint64(b[i:])
	i += 8
	r.Simhash = binary.LittleEndian.Uint64(b[i:])
	i += 8
	r.HTTPStatus = binary.LittleEndian.Uint16(b[i:])
	i += 2
	r.RedirectRef = binary.LittleEndian.Uint64(b[i:])
	i += 8
	r.RetryCount = b[i]
	i++
	r.ErrorCount = binary.LittleEndian.Uint16(b[i:])
	return r
}
