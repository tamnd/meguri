package format

import (
	"bytes"
	"reflect"
	"testing"

	m "github.com/tamnd/meguri"
)

// TestPageSplitSinglePageByteIdentical proves the opt-in is a true no-op until a
// column actually spills: a MaxPageRows at or above the row count leaves every
// column on one page, so the bytes match a partition that never set the field.
// This is what keeps the pinned size baselines and the golden file from moving
// for a partition that does not split.
func TestPageSplitSinglePageByteIdentical(t *testing.T) {
	base := buildPartition(t, CodecZstd)
	a, err := Encode(base)
	if err != nil {
		t.Fatalf("encode default: %v", err)
	}

	wide := buildPartition(t, CodecZstd)
	wide.MaxPageRows = len(wide.URLs) + 1000 // larger than any column, so still one page
	b, err := Encode(wide)
	if err != nil {
		t.Fatalf("encode wide: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("a MaxPageRows above the row count changed the bytes: %d vs %d", len(a), len(b))
	}
}

// TestPageSplitRoundTrip splits a small partition into several pages and requires
// the decode to reconstruct exactly the same records, plus a byte-stable
// re-encode. It confirms the multi-page footer skip list and the per-page cascade
// decode compose back into the original rows.
func TestPageSplitRoundTrip(t *testing.T) {
	p := buildPartition(t, CodecZstd)
	p.MaxPageRows = 2 // 6 url rows -> 3 pages

	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// The hostkey column must really be multi-page, otherwise the test proves
	// nothing about the skip list.
	rd, err := NewReader(enc)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if _, total := rd.HostRangePageScan(0, ^uint64(0)); total <= 1 {
		t.Fatalf("hostkey column has %d pages, want it split into several", total)
	}

	enc2, err := Encode(p)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(enc, enc2) {
		t.Fatalf("multi-page encode is not deterministic")
	}

	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Decode does not carry the writer-side split knob; the records are what must
	// match, so compare with MaxPageRows cleared.
	p.MaxPageRows = 0
	if !reflect.DeepEqual(p, got) {
		t.Fatalf("multi-page round trip mismatch\n want %+v\n got  %+v", p, got)
	}
}

// TestPageSplitPruneCorpus is the real-data gate on the per-page skip list: it
// splits a frozen ccrawl slice into many pages, then reads one host's URLKeys
// back two ways and requires they agree, while the page-pruned read decodes only
// a fraction of the column. Because the URL table is sorted by host key, a single
// host occupies a contiguous run of pages, so the zone check skips the rest.
func TestPageSplitPruneCorpus(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(t, path)
	if len(p.URLs) == 0 || len(p.Hosts) < 3 {
		t.Fatalf("corpus too small: %d urls, %d hosts", len(p.URLs), len(p.Hosts))
	}
	p.MaxPageRows = 4096

	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Re-encode must stay byte-stable even with splitting on.
	enc2, err := Encode(p)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(enc, enc2) {
		t.Fatalf("split corpus encode is not deterministic")
	}

	// Decode reconstructs the same number of rows.
	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.URLs) != len(p.URLs) || len(got.Hosts) != len(p.Hosts) {
		t.Fatalf("counts changed: urls %d->%d hosts %d->%d",
			len(p.URLs), len(got.URLs), len(p.Hosts), len(got.Hosts))
	}

	rd, err := NewReader(enc)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}

	// A whole-column read is the oracle the pruned read must match.
	allKeys, err := rd.URLKeys()
	if err != nil {
		t.Fatalf("url keys: %v", err)
	}

	// Pick the host with the fewest rows so a sharp pruning is visible: a small
	// host occupies a short contiguous run of pages, and every page outside that run
	// is skipped on its zone alone.
	counts := make(map[uint64]int, len(p.Hosts))
	for _, k := range allKeys {
		counts[k.HostKey]++
	}
	var hk uint64
	best := -1
	for key, n := range counts {
		if best < 0 || n < best {
			best, hk = n, key
		}
	}
	var want []m.URLKey
	for _, k := range allKeys {
		if k.HostKey == hk {
			want = append(want, k)
		}
	}
	if len(want) == 0 {
		t.Fatalf("smallest host %x has no urls", hk)
	}

	gotKeys, err := rd.HostRangeURLKeys(hk, hk)
	if err != nil {
		t.Fatalf("host range keys: %v", err)
	}
	if !sameKeySet(want, gotKeys) {
		t.Fatalf("page-pruned read returned %d keys, want %d for host %x", len(gotKeys), len(want), hk)
	}

	scanned, total := rd.HostRangePageScan(hk, hk)
	if total <= 1 {
		t.Fatalf("hostkey column is not multi-page (total=%d); raise corpus size or lower MaxPageRows", total)
	}
	if scanned >= total {
		t.Fatalf("no pages pruned for a single host: scanned %d of %d", scanned, total)
	}
	t.Logf("page pruning: host %x decoded %d of %d hostkey pages for %d urls", hk, scanned, total, len(gotKeys))

	// The single-page fallback must return the same set: a partition that did not
	// opt into splitting still answers the range read correctly.
	flat := loadCorpus(t, path)
	flatEnc, err := Encode(flat)
	if err != nil {
		t.Fatalf("encode flat: %v", err)
	}
	frd, err := NewReader(flatEnc)
	if err != nil {
		t.Fatalf("flat reader: %v", err)
	}
	flatKeys, err := frd.HostRangeURLKeys(hk, hk)
	if err != nil {
		t.Fatalf("flat host range keys: %v", err)
	}
	if !sameKeySet(want, flatKeys) {
		t.Fatalf("single-page fallback returned %d keys, want %d", len(flatKeys), len(want))
	}
	if _, ftotal := frd.HostRangePageScan(hk, hk); ftotal != 0 {
		t.Fatalf("single-page column reported %d skip-list pages, want 0", ftotal)
	}
}
