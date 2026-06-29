package format

import m "github.com/tamnd/meguri"

// The schedule index region (doc 10 section 7, decision D13) is the durable form
// of the frontier's resident due-queue: a bucketed timing wheel that groups URL
// rows by their next_due so a scheduler can find due work by reading a bucket
// directory instead of scanning the whole next_due column. It is the persist side
// of the resident wheel the frontier runs in memory (M4); the region id and the
// PageSchedule page kind were reserved from M0, and this is the serialization
// that fills them.
//
// The wheel has three tiers, coarsening as the horizon recedes, matching the
// frontier's resident layout: a near tier of 168 one-hour buckets (a week at
// hour resolution), a mid tier of 90 one-day buckets (a quarter at day
// resolution), and a far tier of 24 thirty-day buckets (two years at month
// resolution), plus one overflow bucket for anything past the far horizon. A row
// lands in the bucket its next_due falls into, measured from the file's earliest
// nonzero next_due (the STATS due_min), so the wheel needs no wall clock to read.
//
// The region is one PageSchedule page. The payload is the base time, the row
// count it covers, the per-bucket counts, and the row indices grouped by bucket
// (ascending within a bucket, delta-varint coded). A reader resolves "what is due
// at now" to "every row in a bucket whose window starts at or before now", a
// superset of the truly-due rows that the next_due column confirms: the wheel
// prunes the scan to the due buckets, it does not replace the exact check.

// Schedule wheel tier shape. These are the bucket counts and widths in hours; the
// resident frontier wheel uses the same shape so the durable form is a snapshot
// of it, not a re-derivation.
const (
	schedNearBuckets = 168 // one-hour buckets, a week
	schedNearWidth   = 1
	schedMidBuckets  = 90 // one-day buckets, a quarter
	schedMidWidth    = 24
	schedFarBuckets  = 24 // thirty-day buckets, two years
	schedFarWidth    = 720

	// Tier spans in hours: how far each tier reaches from its start.
	schedNearSpan = schedNearBuckets * schedNearWidth // 168
	schedMidSpan  = schedMidBuckets * schedMidWidth   // 2160
	schedFarSpan  = schedFarBuckets * schedFarWidth   // 17280

	// Tier offset bases: the hour offset from the wheel base at which each tier
	// begins. The near tier begins at 0.
	schedMidOff      = schedNearSpan                // 168
	schedFarOff      = schedNearSpan + schedMidSpan // 2328
	schedOverflowOff = schedFarOff + schedFarSpan   // 19608

	// Tier bucket-index bases: the bucket index at which each tier begins. These
	// are not the offset bases; only the mid tier's two happen to coincide because
	// the near tier has one bucket per hour.
	schedMidIdx         = schedNearBuckets                                     // 168
	schedFarIdx         = schedNearBuckets + schedMidBuckets                   // 258
	schedOverflowBucket = schedNearBuckets + schedMidBuckets + schedFarBuckets // 282

	// schedBuckets is the total bucket count: near + mid + far + one overflow.
	schedBuckets = schedOverflowBucket + 1 // 283
)

// schedBucketOf maps an offset in hours from the base time to its bucket index.
func schedBucketOf(offset uint32) int {
	if offset < schedNearSpan {
		return int(offset) // near: one bucket per hour
	}
	offset -= schedNearSpan
	if offset < schedMidSpan {
		return schedMidIdx + int(offset/schedMidWidth)
	}
	offset -= schedMidSpan
	if offset < schedFarSpan {
		return schedFarIdx + int(offset/schedFarWidth)
	}
	return schedOverflowBucket
}

// schedBucketStart returns the offset in hours from the base at which bucket b's
// window begins. A row in bucket b has next_due at or after base+schedBucketStart,
// the fact that lets a reader skip a bucket whose window has not opened.
func schedBucketStart(b int) uint32 {
	switch {
	case b < schedMidIdx:
		return uint32(b) * schedNearWidth
	case b < schedFarIdx:
		return schedMidOff + uint32(b-schedMidIdx)*schedMidWidth
	case b < schedOverflowBucket:
		return schedFarOff + uint32(b-schedFarIdx)*schedFarWidth
	default:
		return schedOverflowOff
	}
}

// buildScheduleIndex groups the scheduled URL rows (nonzero next_due) into wheel
// buckets, returning the base time and the per-bucket row-index lists. It returns
// ok=false when no row is scheduled, the signal to omit the region.
func buildScheduleIndex(recs []m.URLRecord) (base uint32, buckets [][]uint32, ok bool) {
	base, _ = dueRange(recs)
	if base == 0 {
		return 0, nil, false
	}
	buckets = make([][]uint32, schedBuckets)
	for i := range recs {
		d := recs[i].NextDue
		if d == 0 {
			continue
		}
		b := schedBucketOf(d - base)
		buckets[b] = append(buckets[b], uint32(i))
	}
	return base, buckets, true
}

// encodeScheduleRegion frames the wheel as a PageSchedule page. The payload is the
// base time, the covered row count, the per-bucket counts, and the row indices
// grouped by bucket and delta-varint coded within each. An unscheduled partition
// produces no region.
func encodeScheduleRegion(recs []m.URLRecord, codec uint8) []byte {
	base, buckets, ok := buildScheduleIndex(recs)
	if !ok {
		return nil
	}
	var w wbuf
	w.u32(base)
	w.u32(uint32(len(recs)))
	w.uvarint(schedBuckets)
	var scheduled uint64
	for _, b := range buckets {
		w.uvarint(uint64(len(b)))
		scheduled += uint64(len(b))
	}
	for _, b := range buckets {
		var prev uint32
		for _, idx := range b {
			w.uvarint(uint64(idx - prev))
			prev = idx
		}
	}
	return writePage(PageSchedule, EncRaw, codec, uint32(scheduled), 0, uint64(base), w.b)
}

// ScheduleIndex is the read view over a schedule region: the wheel base and the
// row indices grouped by bucket. A scheduler asks it which rows could be due at a
// given time and confirms the candidates against the next_due column.
type ScheduleIndex struct {
	base    uint32
	covered uint32
	buckets [][]uint32
}

// decodeScheduleRegion reads a schedule region back into a ScheduleIndex,
// verifying the page CRC and that it is a PageSchedule page.
func decodeScheduleRegion(region []byte) (*ScheduleIndex, error) {
	h, payload, _, err := readPage(region)
	if err != nil {
		return nil, err
	}
	if h.kind != PageSchedule {
		return nil, ErrCorrupt
	}
	r := &rbuf{b: payload}
	base := r.u32()
	covered := r.u32()
	nb := int(r.uvarint())
	if r.fail() || nb != schedBuckets {
		return nil, ErrCorrupt
	}
	counts := make([]int, nb)
	for i := range counts {
		counts[i] = int(r.uvarint())
	}
	buckets := make([][]uint32, nb)
	for i := range buckets {
		if counts[i] == 0 {
			continue
		}
		ids := make([]uint32, counts[i])
		var prev uint32
		for j := range ids {
			prev += uint32(r.uvarint())
			ids[j] = prev
		}
		buckets[i] = ids
	}
	if r.fail() {
		return nil, ErrCorrupt
	}
	return &ScheduleIndex{base: base, covered: covered, buckets: buckets}, nil
}

// Base returns the wheel's base time, the file's earliest nonzero next_due that
// all bucket windows are measured from.
func (s *ScheduleIndex) Base() uint32 { return s.base }

// Covered returns the total URL row count the wheel was built over, the
// denominator for the fraction of rows a due query selects.
func (s *ScheduleIndex) Covered() uint32 { return s.covered }

// DueBuckets returns the row indices of every bucket whose window starts at or
// before now, the wheel's pushdown answer to "what could be due at now". It is a
// superset of the truly-due rows: a row in the boundary bucket (the one now falls
// inside) may have a next_due just past now, so a caller confirms each candidate
// against the next_due column. A now before the base yields nothing, the cheap
// skip for a file whose soonest work is still in the future.
func (s *ScheduleIndex) DueBuckets(now uint32) []uint32 {
	if now < s.base {
		return nil
	}
	offset := now - s.base
	var out []uint32
	for b := range schedBuckets {
		if schedBucketStart(b) > offset {
			break
		}
		out = append(out, s.buckets[b]...)
	}
	return out
}
