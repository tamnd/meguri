package format

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	m "github.com/tamnd/meguri"
)

// buildPartition assembles a small but structurally complete partition: real
// HostKeys derived the way the engine derives them, a string arena the *Ref
// fields point into, rows sorted the way Encode demands. Every field is set to a
// distinct non-zero value so a serialization bug in any column shows up as a
// round-trip mismatch rather than hiding behind a zero.
func buildPartition(t *testing.T, codec uint8) *Partition {
	t.Helper()

	hosts := []string{"example.com", "golang.org", "rust-lang.org"}
	type seed struct {
		host string
		path string
	}
	seeds := []seed{
		{"example.com", "/"},
		{"example.com", "/about"},
		{"example.com", "/blog?page=2"},
		{"golang.org", "/doc"},
		{"golang.org", "/pkg/strings"},
		{"rust-lang.org", "/learn"},
	}

	var arena []byte
	intern := func(s string) uint64 {
		off := uint64(len(arena))
		arena = append(arena, byte(len(s)))
		arena = append(arena, s...)
		return off
	}

	hostRecs := make([]m.HostRecord, 0, len(hosts))
	for i, h := range hosts {
		hk := m.HostKeyOf(h)
		ref := intern(h)
		hostRecs = append(hostRecs, m.HostRecord{
			HostKey:          hk,
			HostRef:          ref,
			Grouping:         m.GroupRegistrableDomain,
			RegistrableRef:   ref,
			ResolvedIP:       [16]byte{10, 0, 0, byte(i + 1)},
			IPExpiry:         1000 + uint32(i),
			RobotsFetched:    2000 + uint32(i),
			RobotsExpiry:     3000 + uint32(i),
			RobotsRef:        uint64(i + 7),
			CrawlDelay:       uint16(10 + i),
			HostNextEligible: 4000 + uint32(i),
			IPNextEligible:   5000 + uint32(i),
			URLBudget:        100 + uint32(i),
			URLCount:         uint32(i + 1),
			DepthCap:         uint16(8 + i),
			HostScore:        float32(i) * 0.25,
			CrawlTotal:       uint32(50 + i),
			ErrorTotal:       uint32(i),
			AvgLatency:       uint16(120 + i),
			Flags:            m.HostFlagRobotsMissing,
		})
	}
	sort.Slice(hostRecs, func(i, j int) bool { return hostRecs[i].HostKey < hostRecs[j].HostKey })

	urlRecs := make([]m.URLRecord, 0, len(seeds))
	for i, s := range seeds {
		key := m.MakeURLKey(s.host, s.path)
		urlRecs = append(urlRecs, m.URLRecord{
			URLKey:          key,
			HostKey:         key.HostKey,
			Status:          m.URLStatus(i % 8),
			Priority:        float32(i) * 0.5,
			Depth:           uint16(i),
			DiscoverySource: m.DiscoverySource(i % 5),
			URLRef:          intern(s.host + s.path),
			FirstSeen:       100 + uint32(i),
			LastCrawled:     200 + uint32(i),
			LastChanged:     300 + uint32(i),
			NextDue:         400 + uint32(i),
			Lambda:          float32(i) * 0.1,
			CrawlCount:      uint32(i + 1),
			ChangeCount:     uint32(i),
			NoChangeStreak:  uint16(i),
			ETagRef:         intern("etag-" + s.path),
			LastModified:    500 + uint32(i),
			ContentFP:       uint64(0xABCD0000 + i),
			Simhash:         uint64(0x1234_0000 + i),
			HTTPStatus:      uint16(200 + i),
			RedirectRef:     uint64(i),
			RetryCount:      uint8(i),
			ErrorCount:      uint16(i),
		})
	}
	sort.Slice(urlRecs, func(i, j int) bool { return urlRecs[i].URLKey.Less(urlRecs[j].URLKey) })

	lo, hi := uint64(0), ^uint64(0)
	if len(hostRecs) > 0 {
		lo, hi = hostRecs[0].HostKey, hostRecs[len(hostRecs)-1].HostKey
	}

	return &Partition{
		ID:           7,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: 482817,
		DefaultCodec: codec,
		URLs:         urlRecs,
		Hosts:        hostRecs,
		Strings:      arena,
		Meta:         map[string]string{"writer": "meguri-test", "corpus": "synthetic"},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, codec := range []uint8{CodecNone, CodecZstd} {
		p := buildPartition(t, codec)
		enc, err := Encode(p)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		got, err := Decode(enc)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !reflect.DeepEqual(p, got) {
			t.Fatalf("codec %d: round trip mismatch\n want %+v\n got  %+v", codec, p, got)
		}
	}
}

func TestByteStable(t *testing.T) {
	p := buildPartition(t, CodecZstd)
	a, err := Encode(p)
	if err != nil {
		t.Fatalf("encode a: %v", err)
	}
	b, err := Encode(p)
	if err != nil {
		t.Fatalf("encode b: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("encode is not deterministic: %d vs %d bytes, first diff differs", len(a), len(b))
	}
	// Re-encoding what we decoded must reproduce the same bytes too.
	got, err := Decode(a)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	c, err := Encode(got)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(a, c) {
		t.Fatalf("decode then encode is not byte-stable")
	}
}

// TestEncodeToFileMatchesEncode pins the streaming file encoder to the in-memory
// one: EncodeToFile must produce the exact bytes Encode produces, across both
// codecs and every optional region (schedule, seen-set, multi-page columns), so
// the bounded-memory checkpoint path can never drift from the gated byte-stable
// format without this catching it.
func TestEncodeToFileMatchesEncode(t *testing.T) {
	seen := make([]byte, 777)
	for i := range seen {
		seen[i] = byte(i*7 + 3)
	}
	cases := []struct {
		name  string
		build func(codec uint8) *Partition
	}{
		{"basic", func(c uint8) *Partition { return buildPartition(t, c) }},
		{"schedule", func(c uint8) *Partition { p := buildPartition(t, c); p.BuildSchedule = true; return p }},
		{"seenset", func(c uint8) *Partition { p := buildPartition(t, c); p.SeenFilter = seen; return p }},
		{"multipage", func(c uint8) *Partition { p := buildPartition(t, c); p.MaxPageRows = 2; return p }},
		{"all", func(c uint8) *Partition {
			p := buildPartition(t, c)
			p.BuildSchedule = true
			p.SeenFilter = seen
			p.MaxPageRows = 2
			return p
		}},
	}
	for _, tc := range cases {
		for _, codec := range []uint8{CodecNone, CodecZstd} {
			p := tc.build(codec)
			want, err := Encode(p)
			if err != nil {
				t.Fatalf("%s codec %d: encode: %v", tc.name, codec, err)
			}
			path := filepath.Join(t.TempDir(), "out.meguri")
			if err := EncodeToFile(path, p); err != nil {
				t.Fatalf("%s codec %d: encode to file: %v", tc.name, codec, err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%s codec %d: read back: %v", tc.name, codec, err)
			}
			if !bytes.Equal(want, got) {
				t.Fatalf("%s codec %d: streamed file differs from Encode (%d vs %d bytes)",
					tc.name, codec, len(want), len(got))
			}
			// And it must decode cleanly, the same partition the in-memory path yields.
			if _, err := Decode(got); err != nil {
				t.Fatalf("%s codec %d: decode streamed file: %v", tc.name, codec, err)
			}
		}
	}
}

func TestMagicBrackets(t *testing.T) {
	p := buildPartition(t, CodecNone)
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if [4]byte(enc[:4]) != Magic {
		t.Fatalf("missing head magic")
	}
	if [4]byte(enc[len(enc)-4:]) != Magic {
		t.Fatalf("missing tail magic")
	}
}

func TestInspectFromTail(t *testing.T) {
	p := buildPartition(t, CodecZstd)
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ins, err := InspectBytes(enc)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if ins.URLCount != uint64(len(p.URLs)) {
		t.Fatalf("url count: got %d want %d", ins.URLCount, len(p.URLs))
	}
	if ins.HostCount != uint64(len(p.Hosts)) {
		t.Fatalf("host count: got %d want %d", ins.HostCount, len(p.Hosts))
	}
	if ins.URLColumns != urlColumnCount {
		t.Fatalf("url columns: got %d want %d", ins.URLColumns, urlColumnCount)
	}
	if ins.HostColumns != hostColumnCount {
		t.Fatalf("host columns: got %d want %d", ins.HostColumns, hostColumnCount)
	}
	if ins.PartitionID != p.ID {
		t.Fatalf("partition id: got %d want %d", ins.PartitionID, p.ID)
	}
	if ins.String() == "" {
		t.Fatalf("inspect string is empty")
	}
}

func TestRejectsUnsorted(t *testing.T) {
	p := buildPartition(t, CodecNone)
	if len(p.URLs) >= 2 {
		p.URLs[0], p.URLs[1] = p.URLs[1], p.URLs[0]
	}
	if _, err := Encode(p); !errors.Is(err, ErrNotSorted) {
		t.Fatalf("want ErrNotSorted, got %v", err)
	}
}

func TestDetectsCorruption(t *testing.T) {
	p := buildPartition(t, CodecNone)
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Flip a byte inside the URL region (just past the header) and expect a
	// checksum failure rather than silent bad data.
	corrupt := append([]byte(nil), enc...)
	corrupt[HeaderSize+40] ^= 0xFF
	if _, err := Decode(corrupt); err == nil {
		t.Fatalf("decode accepted corrupted file")
	}
}

func TestEmptyPartition(t *testing.T) {
	p := &Partition{ID: 1, DefaultCodec: CodecNone}
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}
	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(got.URLs) != 0 || len(got.Hosts) != 0 {
		t.Fatalf("empty partition came back non-empty")
	}
}
