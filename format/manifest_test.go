package format

import (
	"reflect"
	"testing"
)

func sampleManifest() *Manifest {
	return BuildManifest([]ManifestEntry{
		{PartitionID: 3, FileRef: "p3.meguri", HostKeyLo: 0x8000, HostKeyHi: 0xffff, URLCount: 30, DueMin: 470100, BytesPerURL: 41.5, Epoch: 1},
		{PartitionID: 1, FileRef: "p1.meguri", HostKeyLo: 0x0000, HostKeyHi: 0x3fff, URLCount: 10, DueMin: 0, BytesPerURL: 44.0, Epoch: 1},
		{PartitionID: 2, FileRef: "p2.meguri", HostKeyLo: 0x4000, HostKeyHi: 0x7fff, URLCount: 20, DueMin: 470050, BytesPerURL: 39.0, Epoch: 1},
	})
}

func TestManifestSortAndRoute(t *testing.T) {
	man := sampleManifest()
	if man.Entries[0].PartitionID != 1 || man.Entries[2].PartitionID != 3 {
		t.Fatalf("entries not sorted by HostKeyLo: %+v", man.Entries)
	}
	cases := []struct {
		hk   uint64
		want uint32
		ok   bool
	}{
		{0x0000, 1, true},
		{0x3fff, 1, true},
		{0x4000, 2, true},
		{0x9999, 3, true},
		{0xffff, 3, true},
	}
	for _, c := range cases {
		e, ok := man.Route(c.hk)
		if ok != c.ok || (ok && e.PartitionID != c.want) {
			t.Fatalf("route 0x%x: got (%d,%v) want (%d,%v)", c.hk, e.PartitionID, ok, c.want, c.ok)
		}
	}
}

func TestManifestRouteGap(t *testing.T) {
	man := BuildManifest([]ManifestEntry{
		{PartitionID: 1, HostKeyLo: 0x10, HostKeyHi: 0x1f, Epoch: 1},
		{PartitionID: 2, HostKeyLo: 0x30, HostKeyHi: 0x3f, Epoch: 1},
	})
	if _, ok := man.Route(0x05); ok {
		t.Fatalf("route below the first range should miss")
	}
	if _, ok := man.Route(0x25); ok {
		t.Fatalf("route into the gap should miss")
	}
}

func TestManifestDueParts(t *testing.T) {
	man := sampleManifest()
	due := man.DueParts(470080)
	if len(due) != 1 || due[0].PartitionID != 2 {
		t.Fatalf("due at 470080: want only p2, got %+v", due)
	}
	due = man.DueParts(470200)
	if len(due) != 2 {
		t.Fatalf("due at 470200: want p2 and p3, got %d", len(due))
	}
}

func TestManifestCoverage(t *testing.T) {
	full := BuildManifest([]ManifestEntry{
		{PartitionID: 1, HostKeyLo: 0, HostKeyHi: 0x7fff_ffff_ffff_ffff, Epoch: 1},
		{PartitionID: 2, HostKeyLo: 0x8000_0000_0000_0000, HostKeyHi: ^uint64(0), Epoch: 1},
	})
	if _, _, gap := full.CoverageGap(1); gap {
		t.Fatalf("full coverage reported a gap")
	}
	holed := BuildManifest([]ManifestEntry{
		{PartitionID: 1, HostKeyLo: 0, HostKeyHi: 0x10, Epoch: 1},
		{PartitionID: 2, HostKeyLo: 0x20, HostKeyHi: ^uint64(0), Epoch: 1},
	})
	lo, hi, gap := holed.CoverageGap(1)
	if !gap || lo != 0x11 || hi != 0x1f {
		t.Fatalf("want gap [0x11,0x1f], got [%x,%x] gap=%v", lo, hi, gap)
	}
}

func TestManifestEncodeRoundTrip(t *testing.T) {
	man := sampleManifest()
	enc := EncodeManifest(man)
	got, err := DecodeManifest(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(man, got) {
		t.Fatalf("manifest round trip mismatch\n want %+v\n got  %+v", man, got)
	}
	// A flipped byte in the body must fail the CRC.
	enc[8] ^= 0xff
	if _, err := DecodeManifest(enc); err == nil {
		t.Fatalf("decode accepted a corrupted manifest")
	}
}

func TestManifestEntryFromFile(t *testing.T) {
	p := buildPartition(t, CodecZstd)
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	e, err := ManifestEntryFor(enc, "part.meguri", 5)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	if e.PartitionID != p.ID || e.HostKeyLo != p.HostKeyLo || e.HostKeyHi != p.HostKeyHi {
		t.Fatalf("entry identity mismatch: %+v", e)
	}
	if e.URLCount != uint64(len(p.URLs)) || e.Epoch != 5 || e.FileRef != "part.meguri" {
		t.Fatalf("entry fields mismatch: %+v", e)
	}
}
