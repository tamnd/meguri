package store

import (
	"encoding/binary"
	"math"

	"github.com/tamnd/meguri"
)

// The per-record codec encodes a URLRecord or HostRecord into the fixed-width
// value bytes a log frame carries (section 3.3 of doc 11): every field is
// fixed-width or a fixed-width reference into the string arena, so a record's
// serialized size depends only on its schema, not its content. That is what
// makes the same-size in-place update the common case and what lets the
// checkpoint stream records straight into doc 10's columns with no remap.
//
// The URLKey and the HostKey are not encoded in the value: the log frame holds
// the key separately, so decode takes the key from the frame and fills the
// record's key fields. This keeps the value the pure payload, the way hashlog's
// opaque value sits under the engine.

// urlValueSize is the fixed serialized length of a URLRecord value.
const urlValueSize = 1 + 4 + 2 + 1 + // Status, Priority, Depth, DiscoverySource
	8 + // URLRef
	4 + 4 + 4 + 4 + // FirstSeen, LastCrawled, LastChanged, NextDue
	4 + 4 + 4 + 2 + // Lambda, CrawlCount, ChangeCount, NoChangeStreak
	8 + 4 + // ETagRef, LastModified
	8 + 8 + // ContentFP, Simhash
	2 + 8 + // HTTPStatus, RedirectRef
	1 + 2 // RetryCount, ErrorCount

// hostValueSize is the fixed serialized length of a HostRecord value.
const hostValueSize = 8 + 1 + 8 + // HostRef, Grouping, RegistrableRef
	16 + 4 + // ResolvedIP, IPExpiry
	4 + 4 + 8 + 2 + // RobotsFetched, RobotsExpiry, RobotsRef, CrawlDelay
	4 + 4 + // HostNextEligible, IPNextEligible
	4 + 4 + 2 + // URLBudget, URLCount, DepthCap
	4 + // HostScore
	4 + 4 + 2 + // CrawlTotal, ErrorTotal, AvgLatency
	2 // Flags

// encodeURL writes a URLRecord's value bytes into dst, which must be at least
// urlValueSize long. The URLKey is omitted; the frame carries it.
func encodeURL(dst []byte, r *meguri.URLRecord) {
	w := vwriter{b: dst}
	w.u8(uint8(r.Status))
	w.f32(r.Priority)
	w.u16(r.Depth)
	w.u8(uint8(r.DiscoverySource))
	w.u64(r.URLRef)
	w.u32(r.FirstSeen)
	w.u32(r.LastCrawled)
	w.u32(r.LastChanged)
	w.u32(r.NextDue)
	w.f32(r.Lambda)
	w.u32(r.CrawlCount)
	w.u32(r.ChangeCount)
	w.u16(r.NoChangeStreak)
	w.u64(r.ETagRef)
	w.u32(r.LastModified)
	w.u64(r.ContentFP)
	w.u64(r.Simhash)
	w.u16(r.HTTPStatus)
	w.u64(r.RedirectRef)
	w.u8(r.RetryCount)
	w.u16(r.ErrorCount)
}

// decodeURL reads a URLRecord value back, filling the key fields from key.
func decodeURL(key meguri.URLKey, b []byte) meguri.URLRecord {
	r := vreader{b: b}
	rec := meguri.URLRecord{URLKey: key, HostKey: key.HostKey}
	rec.Status = meguri.URLStatus(r.u8())
	rec.Priority = r.f32()
	rec.Depth = r.u16()
	rec.DiscoverySource = meguri.DiscoverySource(r.u8())
	rec.URLRef = r.u64()
	rec.FirstSeen = r.u32()
	rec.LastCrawled = r.u32()
	rec.LastChanged = r.u32()
	rec.NextDue = r.u32()
	rec.Lambda = r.f32()
	rec.CrawlCount = r.u32()
	rec.ChangeCount = r.u32()
	rec.NoChangeStreak = r.u16()
	rec.ETagRef = r.u64()
	rec.LastModified = r.u32()
	rec.ContentFP = r.u64()
	rec.Simhash = r.u64()
	rec.HTTPStatus = r.u16()
	rec.RedirectRef = r.u64()
	rec.RetryCount = r.u8()
	rec.ErrorCount = r.u16()
	return rec
}

// encodeHost writes a HostRecord's value bytes into dst, at least hostValueSize.
func encodeHost(dst []byte, r *meguri.HostRecord) {
	w := vwriter{b: dst}
	w.u64(r.HostRef)
	w.u8(uint8(r.Grouping))
	w.u64(r.RegistrableRef)
	w.raw(r.ResolvedIP[:])
	w.u32(r.IPExpiry)
	w.u32(r.RobotsFetched)
	w.u32(r.RobotsExpiry)
	w.u64(r.RobotsRef)
	w.u16(r.CrawlDelay)
	w.u32(r.HostNextEligible)
	w.u32(r.IPNextEligible)
	w.u32(r.URLBudget)
	w.u32(r.URLCount)
	w.u16(r.DepthCap)
	w.f32(r.HostScore)
	w.u32(r.CrawlTotal)
	w.u32(r.ErrorTotal)
	w.u16(r.AvgLatency)
	w.u16(r.Flags)
}

// decodeHost reads a HostRecord value back, filling HostKey from key.
func decodeHost(key uint64, b []byte) meguri.HostRecord {
	r := vreader{b: b}
	rec := meguri.HostRecord{HostKey: key}
	rec.HostRef = r.u64()
	rec.Grouping = meguri.HostGrouping(r.u8())
	rec.RegistrableRef = r.u64()
	r.rawInto(rec.ResolvedIP[:])
	rec.IPExpiry = r.u32()
	rec.RobotsFetched = r.u32()
	rec.RobotsExpiry = r.u32()
	rec.RobotsRef = r.u64()
	rec.CrawlDelay = r.u16()
	rec.HostNextEligible = r.u32()
	rec.IPNextEligible = r.u32()
	rec.URLBudget = r.u32()
	rec.URLCount = r.u32()
	rec.DepthCap = r.u16()
	rec.HostScore = r.f32()
	rec.CrawlTotal = r.u32()
	rec.ErrorTotal = r.u32()
	rec.AvgLatency = r.u16()
	rec.Flags = r.u16()
	return rec
}

// vwriter is a position-tracking little-endian writer over a fixed slice, the
// byte order the fleet uses everywhere (doc 03).
type vwriter struct {
	b []byte
	i int
}

func (w *vwriter) u8(v uint8)    { w.b[w.i] = v; w.i++ }
func (w *vwriter) u16(v uint16)  { binary.LittleEndian.PutUint16(w.b[w.i:], v); w.i += 2 }
func (w *vwriter) u32(v uint32)  { binary.LittleEndian.PutUint32(w.b[w.i:], v); w.i += 4 }
func (w *vwriter) u64(v uint64)  { binary.LittleEndian.PutUint64(w.b[w.i:], v); w.i += 8 }
func (w *vwriter) f32(v float32) { w.u32(math.Float32bits(v)) }
func (w *vwriter) raw(p []byte)  { copy(w.b[w.i:], p); w.i += len(p) }

// vreader is the matching reader.
type vreader struct {
	b []byte
	i int
}

func (r *vreader) u8() uint8        { v := r.b[r.i]; r.i++; return v }
func (r *vreader) u16() uint16      { v := binary.LittleEndian.Uint16(r.b[r.i:]); r.i += 2; return v }
func (r *vreader) u32() uint32      { v := binary.LittleEndian.Uint32(r.b[r.i:]); r.i += 4; return v }
func (r *vreader) u64() uint64      { v := binary.LittleEndian.Uint64(r.b[r.i:]); r.i += 8; return v }
func (r *vreader) f32() float32     { return math.Float32frombits(r.u32()) }
func (r *vreader) rawInto(p []byte) { copy(p, r.b[r.i:r.i+len(p)]); r.i += len(p) }
