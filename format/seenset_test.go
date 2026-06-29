package format

import (
	"bytes"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// TestSeensetRegionRoundTrip checks the seen-set filter region carries an opaque
// blob through Encode/Decode byte for byte, sets the region flag, and lists the
// region in the footer. The format treats the blob as opaque, so an arbitrary
// byte pattern is the right input to test the framing alone.
func TestSeensetRegionRoundTrip(t *testing.T) {
	blob := make([]byte, 777)
	for i := range blob {
		blob[i] = byte(i*7 + 3)
	}
	p := &Partition{
		ID:        1,
		HostKeyLo: 0,
		HostKeyHi: ^uint64(0),
		URLs: []meguri.URLRecord{
			{URLKey: meguri.URLKey{HostKey: 1, PathKey: 1}, HostKey: 1, Status: meguri.StatusScheduled},
		},
		Hosts:      []meguri.HostRecord{{HostKey: 1, CrawlDelay: 10}},
		SeenFilter: blob,
	}

	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got.SeenFilter, blob) {
		t.Fatalf("seen-set blob not preserved: got %d bytes, want %d", len(got.SeenFilter), len(blob))
	}

	ins, err := InspectBytes(enc)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if ins.Flags&FlagHasSeenset == 0 {
		t.Fatal("FlagHasSeenset not set")
	}
	var found bool
	for _, r := range ins.Regions {
		if r.ID == RegionSeenset {
			found = true
		}
	}
	if !found {
		t.Fatal("seenset region not listed in the footer")
	}

	// Re-encoding the decoded partition is byte-stable.
	enc2, err := Encode(got)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(enc, enc2) {
		t.Fatal("re-encode with a seen-set region is not byte-stable")
	}
}

// TestNoSeensetRegionByteStable checks a partition without a filter is byte-for-byte
// what it was before the region existed: the region and its flag are absent, so the
// golden-bytes guard and every prior gate are unaffected.
func TestNoSeensetRegionByteStable(t *testing.T) {
	p := &Partition{
		ID: 1, HostKeyHi: ^uint64(0),
		URLs:  []meguri.URLRecord{{URLKey: meguri.URLKey{HostKey: 1, PathKey: 1}, HostKey: 1}},
		Hosts: []meguri.HostRecord{{HostKey: 1, CrawlDelay: 10}},
	}
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ins, err := InspectBytes(enc)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if ins.Flags&FlagHasSeenset != 0 {
		t.Fatal("FlagHasSeenset set on a partition with no filter")
	}
	for _, r := range ins.Regions {
		if r.ID == RegionSeenset {
			t.Fatal("seenset region present on a partition with no filter")
		}
	}
}

// TestCorpusSeensetRegion is the real-data gate for the seen-set filter region:
// build the resident filter over every canonical key from the frozen
// CC-MAIN-2026-25 slice, carry it into the partition, encode and decode the whole
// .meguri file, reconstruct the filter from the decoded region, and require it to
// answer identically for every corpus key. It proves the filter survives the
// real on-disk round trip, not just an in-memory marshal.
func TestCorpusSeensetRegion(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(t, path)
	if len(p.URLs) == 0 {
		t.Fatalf("corpus %s produced no url records", path)
	}

	s := dedup.NewSeenSet(dedup.WithCapacity(uint64(len(p.URLs))))
	keys := make([]meguri.URLKey, len(p.URLs))
	for i := range p.URLs {
		keys[i] = p.URLs[i].URLKey
		s.Seen(keys[i])
	}
	p.SeenFilter = s.MarshalFilter()

	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.SeenFilter) == 0 {
		t.Fatal("decoded partition carries no seen-set region")
	}

	rf, err := dedup.UnmarshalFilter(got.SeenFilter)
	if err != nil {
		t.Fatalf("reconstruct filter: %v", err)
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("corpus key %d went missing through the on-disk round trip", i)
		}
	}
	ins, _ := InspectBytes(enc)
	t.Logf("corpus seen-set region: %d keys, %d region bytes (%.2f bytes/url), file %.2f bytes/url",
		s.Len(), len(got.SeenFilter), float64(len(got.SeenFilter))/float64(s.Len()), ins.Stats.BytesPerURL)
}
