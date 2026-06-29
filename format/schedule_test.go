package format

import (
	"testing"

	m "github.com/tamnd/meguri"
)

// TestScheduleBucketMath pins the wheel's tier boundaries: the near, mid, and far
// tiers must partition the offset space with no gap or overlap, and a bucket's
// start must be the inverse of the mapping that fills it.
func TestScheduleBucketMath(t *testing.T) {
	cases := []struct {
		offset uint32
		bucket int
	}{
		{0, 0}, {1, 1}, {167, 167}, // near, one bucket per hour
		{168, 168}, {191, 168}, {192, 169}, // mid, one bucket per day
		{2327, 257},                           // last mid hour
		{2328, 258}, {3047, 258}, {3048, 259}, // far, one bucket per 30 days
		{19607, 281},                 // last far hour
		{19608, 282}, {1 << 30, 282}, // overflow
	}
	for _, c := range cases {
		if got := schedBucketOf(c.offset); got != c.bucket {
			t.Errorf("schedBucketOf(%d) = %d, want %d", c.offset, got, c.bucket)
		}
	}
	// A bucket's start maps back into that bucket, the round-trip the pushdown
	// relies on, and starts are strictly increasing so DueBuckets can stop early.
	var prev uint32
	for b := range schedBuckets {
		s := schedBucketStart(b)
		if b > 0 && s <= prev {
			t.Fatalf("bucket %d start %d not greater than previous %d", b, s, prev)
		}
		if b < schedOverflowBucket && schedBucketOf(s) != b {
			t.Fatalf("schedBucketOf(start(%d)=%d) = %d, want %d", b, s, schedBucketOf(s), b)
		}
		prev = s
	}
}

// TestScheduleRegionRoundTrip checks an opt-in schedule region serializes, reads
// back through a Reader, and that the wheel-pruned due read returns exactly what
// the column-scan due read returns at a range of now values.
func TestScheduleRegionRoundTrip(t *testing.T) {
	p := smallPartition()
	p.BuildSchedule = true
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	r, err := NewReader(enc)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if !r.HasSchedule() {
		t.Fatal("HasSchedule false after BuildSchedule")
	}
	idx, err := r.Schedule()
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if idx == nil {
		t.Fatal("Schedule returned nil with a region present")
	}
	if idx.Base() != 100 || idx.Covered() != 4 {
		t.Fatalf("wheel base=%d covered=%d, want 100 and 4", idx.Base(), idx.Covered())
	}

	for _, now := range []uint32{99, 100, 174, 175, 250, 300} {
		wheel, err := r.DueByWheel(now)
		if err != nil {
			t.Fatalf("DueByWheel(%d): %v", now, err)
		}
		scan, err := r.DueKeys(now)
		if err != nil {
			t.Fatalf("DueKeys(%d): %v", now, err)
		}
		if !sameKeySet(wheel, scan) {
			t.Fatalf("now=%d: wheel %v != scan %v", now, wheel, scan)
		}
	}
	if got, _ := r.DueByWheel(99); got != nil {
		t.Fatalf("DueByWheel(99) = %v, want nil (before base)", got)
	}
}

// TestScheduleByteStableReencode checks the BuildSchedule directive round-trips
// through a decode: a file with a schedule region decodes with the flag set, so a
// re-encode reproduces the same bytes.
func TestScheduleByteStableReencode(t *testing.T) {
	p := smallPartition()
	p.BuildSchedule = true
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !dec.BuildSchedule {
		t.Fatal("decode did not recover BuildSchedule from the schedule region")
	}
	re, err := Encode(dec)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if len(re) != len(enc) || crc32c(re) != crc32c(enc) {
		t.Fatalf("re-encode diverged: len %d crc %08x vs len %d crc %08x",
			len(re), crc32c(re), len(enc), crc32c(enc))
	}
}

// TestNoScheduleRegionByteStable checks a partition that does not opt in carries
// no schedule region and that the wheel-pruned read falls back to the column scan.
func TestNoScheduleRegionByteStable(t *testing.T) {
	p := smallPartition()
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	r, err := NewReader(enc)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if r.HasSchedule() {
		t.Fatal("HasSchedule true without BuildSchedule")
	}
	idx, err := r.Schedule()
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if idx != nil {
		t.Fatal("Schedule returned a wheel with no region")
	}
	// DueByWheel falls back to the column scan when no wheel is present.
	wheel, _ := r.DueByWheel(175)
	scan, _ := r.DueKeys(175)
	if !sameKeySet(wheel, scan) {
		t.Fatalf("fallback diverged: %v != %v", wheel, scan)
	}
}

// TestCorpusScheduleRegion is the real-data gate for the schedule index (doc 10
// section 7): over the frozen ccrawl slice the wheel-pruned due read must agree
// with a brute-force next_due filter at a now inside the due range, and the wheel
// must prune the scan to a fraction of the rows. It reports the region size and
// the selectivity, the reason the wheel exists.
func TestCorpusScheduleRegion(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(t, path)
	if len(p.URLs) == 0 {
		t.Fatalf("corpus %s produced no url records", path)
	}
	p.BuildSchedule = true
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	full, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	r, err := NewReader(enc)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	idx, err := r.Schedule()
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if idx == nil {
		t.Fatal("no schedule region after BuildSchedule on the corpus")
	}

	dmin, dmax := r.DueRange()
	now := dmin + (dmax-dmin)/2

	got, err := r.DueByWheel(now)
	if err != nil {
		t.Fatalf("DueByWheel: %v", err)
	}
	want := 0
	for i := range full.URLs {
		if d := full.URLs[i].NextDue; d != 0 && d <= now {
			want++
		}
	}
	if len(got) != want {
		t.Fatalf("DueByWheel(%d) returned %d, brute force says %d", now, len(got), want)
	}

	// The wheel pruned the scan: the candidate buckets cover only the rows whose
	// window has opened, a fraction of the table, and that candidate set is a
	// superset of the truly-due rows (no due row missed).
	cand := idx.DueBuckets(now)
	if len(cand) < want {
		t.Fatalf("wheel candidates %d fewer than due rows %d, the superset broke", len(cand), want)
	}
	region, _ := findRegion(r.footer.regions, RegionSchedule)
	t.Logf("corpus schedule: %d rows, region %d bytes (%.2f bytes/url), due@%d scanned %d candidates (%.1f%%) for %d due rows",
		len(p.URLs), region.length, float64(region.length)/float64(len(p.URLs)),
		now, len(cand), 100*float64(len(cand))/float64(len(p.URLs)), want)
}

// sameKeySet reports whether two URLKey slices hold the same keys regardless of
// order.
func sameKeySet(a, b []m.URLKey) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[m.URLKey]int, len(a))
	for _, k := range a {
		seen[k]++
	}
	for _, k := range b {
		seen[k]--
		if seen[k] < 0 {
			return false
		}
	}
	return true
}
