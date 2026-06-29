package format

import (
	"testing"

	m "github.com/tamnd/meguri"
)

// urlColBytes sums the compressed on-disk size of the named URL columns, the
// measure of what a projection actually reads.
func (r *Reader) urlColBytes(cols ...int) uint64 {
	want := map[int]bool{}
	for _, c := range cols {
		want[c] = true
	}
	var total uint64
	for _, d := range r.footer.urlDir {
		if want[d.columnID] {
			total += d.totalCompressed
		}
	}
	return total
}

func smallPartition() *Partition {
	urls := []m.URLRecord{
		{URLKey: m.URLKey{HostKey: 10, PathKey: 1}, HostKey: 10, Status: m.StatusScheduled, NextDue: 100},
		{URLKey: m.URLKey{HostKey: 10, PathKey: 2}, HostKey: 10, Status: m.StatusCrawled, NextDue: 0},
		{URLKey: m.URLKey{HostKey: 20, PathKey: 1}, HostKey: 20, Status: m.StatusScheduled, NextDue: 250},
		{URLKey: m.URLKey{HostKey: 20, PathKey: 2}, HostKey: 20, Status: m.StatusDueRecrawl, NextDue: 175},
	}
	return &Partition{
		ID: 1, HostKeyLo: 10, HostKeyHi: 20,
		URLs:  urls,
		Hosts: []m.HostRecord{{HostKey: 10, CrawlDelay: 10}, {HostKey: 20, CrawlDelay: 10}},
	}
}

// TestReaderProjection checks the projected key and next_due columns reconstruct
// exactly what a full Decode produces, the correctness floor under the cheaper
// read path.
func TestReaderProjection(t *testing.T) {
	p := smallPartition()
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

	keys, err := r.URLKeys()
	if err != nil {
		t.Fatalf("URLKeys: %v", err)
	}
	if len(keys) != len(full.URLs) {
		t.Fatalf("projected %d keys, full decode has %d", len(keys), len(full.URLs))
	}
	for i := range keys {
		if keys[i] != full.URLs[i].URLKey {
			t.Fatalf("key %d diverged: %v vs %v", i, keys[i], full.URLs[i].URLKey)
		}
	}

	due, err := r.NextDue()
	if err != nil {
		t.Fatalf("NextDue: %v", err)
	}
	for i := range due {
		if due[i] != full.URLs[i].NextDue {
			t.Fatalf("next_due %d diverged: %d vs %d", i, due[i], full.URLs[i].NextDue)
		}
	}
}

// TestReaderDuePushdown checks DueKeys returns exactly the scheduled rows due at
// or before now and that the file-level pushdown skips a file with nothing due
// without decoding a column.
func TestReaderDuePushdown(t *testing.T) {
	p := smallPartition()
	enc, _ := Encode(p)
	r, err := NewReader(enc)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}

	dmin, dmax := r.DueRange()
	if dmin != 100 || dmax != 250 {
		t.Fatalf("due range = [%d,%d], want [100,250] (the nonzero next_due span)", dmin, dmax)
	}

	// now before the soonest due: the pushdown skips the file.
	if r.MaybeDueAt(99) {
		t.Fatal("MaybeDueAt(99) true, but the soonest due is 100")
	}
	if keys, _ := r.DueKeys(99); keys != nil {
		t.Fatalf("DueKeys(99) returned %d keys, want none (pushed down)", len(keys))
	}

	// now at 175: the two rows due at 100 and 175, not the one at 250 or the
	// unscheduled one.
	keys, err := r.DueKeys(175)
	if err != nil {
		t.Fatalf("DueKeys: %v", err)
	}
	want := map[m.URLKey]bool{
		{HostKey: 10, PathKey: 1}: true,
		{HostKey: 20, PathKey: 2}: true,
	}
	if len(keys) != len(want) {
		t.Fatalf("DueKeys(175) returned %d keys, want %d", len(keys), len(want))
	}
	for _, k := range keys {
		if !want[k] {
			t.Fatalf("DueKeys(175) returned unexpected key %v", k)
		}
	}
}

// TestReaderHostPushdown checks the header HostKey range prunes a host the
// partition cannot own.
func TestReaderHostPushdown(t *testing.T) {
	p := smallPartition()
	enc, _ := Encode(p)
	r, _ := NewReader(enc)

	lo, hi := r.HostKeyRange()
	if lo != 10 || hi != 20 {
		t.Fatalf("host range = [%d,%d], want [10,20]", lo, hi)
	}
	if r.MaybeOwnsHost(5) || r.MaybeOwnsHost(21) {
		t.Fatal("MaybeOwnsHost accepted a host outside the range")
	}
	if !r.MaybeOwnsHost(10) || !r.MaybeOwnsHost(15) || !r.MaybeOwnsHost(20) {
		t.Fatal("MaybeOwnsHost rejected a host inside the range")
	}
}

// TestCorpusReaderProjection is the real-data gate for predicate pushdown and
// projection discipline (doc 10 section 9): over the frozen CC-MAIN-2026-25
// slice, the urlkey-only projection must reconstruct exactly the keys a full
// decode produces while reading only the two key columns, and the due scan must
// agree with a brute-force filter. It reports the projection's byte savings, the
// reason the discipline exists.
func TestCorpusReaderProjection(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(t, path)
	if len(p.URLs) == 0 {
		t.Fatalf("corpus %s produced no url records", path)
	}
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

	keys, err := r.URLKeys()
	if err != nil {
		t.Fatalf("URLKeys: %v", err)
	}
	if len(keys) != len(full.URLs) {
		t.Fatalf("projected %d keys, full %d", len(keys), len(full.URLs))
	}
	for i := range keys {
		if keys[i] != full.URLs[i].URLKey {
			t.Fatalf("key %d diverged", i)
		}
	}

	// Due scan agrees with a brute-force filter at a now inside the due range.
	dmin, dmax := r.DueRange()
	now := dmin + (dmax-dmin)/2
	got, err := r.DueKeys(now)
	if err != nil {
		t.Fatalf("DueKeys: %v", err)
	}
	want := 0
	for i := range full.URLs {
		if d := full.URLs[i].NextDue; d != 0 && d <= now {
			want++
		}
	}
	if len(got) != want {
		t.Fatalf("DueKeys(%d) returned %d, brute force says %d", now, len(got), want)
	}

	keyBytes := r.urlColBytes(ColURLHostKey, ColURLPathKey)
	t.Logf("corpus projection: %d keys, urlkey columns %d bytes of %d file bytes (%.1f%%), due@%d -> %d rows",
		len(keys), keyBytes, len(enc), 100*float64(keyBytes)/float64(len(enc)), now, len(got))
}
