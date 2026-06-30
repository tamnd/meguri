package format

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	m "github.com/tamnd/meguri"
)

// sliceURLSource is a test URLRecordSource over an in-memory slice.
type sliceURLSource struct {
	recs []m.URLRecord
	i    int
}

func (s *sliceURLSource) Next() (m.URLRecord, bool) {
	if s.i >= len(s.recs) {
		return m.URLRecord{}, false
	}
	r := s.recs[s.i]
	s.i++
	return r, true
}

// makeURLRecords builds n distinct, fully-populated URL records sorted by key, so
// every column carries varied bytes and the cascade-vs-raw page decisions are
// exercised across pages rather than hiding behind a single page of zeros.
func makeURLRecords(n int) []m.URLRecord {
	hosts := []string{"example.com", "golang.org", "rust-lang.org", "a.org", "zzz.net"}
	recs := make([]m.URLRecord, 0, n)
	for i := range n {
		host := hosts[i%len(hosts)]
		key := m.MakeURLKey(host, "/p/"+itoa(i))
		recs = append(recs, m.URLRecord{
			URLKey:          key,
			HostKey:         key.HostKey,
			Status:          m.URLStatus(i % 8),
			Priority:        float32(i%13) * 0.5,
			Depth:           uint16(i % 40),
			DiscoverySource: m.DiscoverySource(i % 5),
			URLRef:          uint64(i) * 17,
			FirstSeen:       100 + uint32(i),
			LastCrawled:     200 + uint32(i%7),
			LastChanged:     300 + uint32(i%5),
			NextDue:         400 + uint32(i),
			Lambda:          float32(i%9) * 0.1,
			CrawlCount:      uint32(i % 11),
			ChangeCount:     uint32(i % 3),
			NoChangeStreak:  uint16(i % 4),
			ETagRef:         uint64(i) * 31,
			LastModified:    500 + uint32(i%6),
			ContentFP:       uint64(0xABCD0000 + i*7),
			Simhash:         uint64(0x12340000 + i*13),
			HTTPStatus:      uint16(200 + i%5),
			RedirectRef:     uint64(i % 2), // sparse, exercises RLE
			RetryCount:      uint8(i % 3),
			ErrorCount:      uint16(i % 2),
		})
	}
	sort.Slice(recs, func(a, b int) bool { return recs[a].URLKey.Less(recs[b].URLKey) })
	return recs
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// TestStreamURLRegionMatchesEncodeColumnRegion is the byte-stability gate for the
// streaming checkpoint (spec 2072 D9, 2071 implementation doc 51): StreamURLRegion
// must produce the exact region bytes and the exact column directory that the
// materializing encodeColumnRegion(urlColumns(...)) produces, across both codecs,
// several row counts (including zero and exact page multiples), and several page
// caps. If the bounded path ever drifts from the gated format, this catches it.
func TestStreamURLRegionMatchesEncodeColumnRegion(t *testing.T) {
	const regionStart = 12345
	for _, codec := range []uint8{CodecNone, CodecZstd} {
		for _, n := range []int{0, 1, 6, 7, 64, 100} {
			for _, maxRows := range []int{0, 1, 3, 16, 50, 1000} {
				recs := makeURLRecords(n)

				wantRegion, wantDir := encodeColumnRegion(urlColumns(recs, codec), regionStart, maxRows)

				var got bytes.Buffer
				gotDir, gotLen, gotCRC, err := StreamURLRegion(&got, &sliceURLSource{recs: recs}, regionStart, maxRows, codec, t.TempDir())
				if err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: stream: %v", codec, n, maxRows, err)
				}

				if int(gotLen) != got.Len() {
					t.Fatalf("codec=%d n=%d maxRows=%d: reported len %d != written %d", codec, n, maxRows, gotLen, got.Len())
				}
				if gotCRC != crc32c(wantRegion) {
					t.Fatalf("codec=%d n=%d maxRows=%d: region CRC %d != %d", codec, n, maxRows, gotCRC, crc32c(wantRegion))
				}
				if !bytes.Equal(got.Bytes(), wantRegion) {
					t.Fatalf("codec=%d n=%d maxRows=%d: region bytes differ (stream %d, want %d)", codec, n, maxRows, got.Len(), len(wantRegion))
				}
				if !reflect.DeepEqual(gotDir, wantDir) {
					t.Fatalf("codec=%d n=%d maxRows=%d: directory differs\n got  %+v\n want %+v", codec, n, maxRows, gotDir, wantDir)
				}
			}
		}
	}
}

// TestStreamEncodeToFileMatchesEncodeToFile pins the whole streaming checkpoint
// encoder to the materializing one: StreamEncodeToFile, fed the same records via a
// source, must produce the exact .meguri bytes EncodeToFile produces from a
// materialized p.URLs, across both codecs, several row counts, and several page
// caps. This is the byte-stability gate the bounded-memory Store.Checkpoint rides
// on; if the streamed snapshot ever drifts from the gated format, this catches it.
func TestStreamEncodeToFileMatchesEncodeToFile(t *testing.T) {
	for _, codec := range []uint8{CodecNone, CodecZstd} {
		for _, n := range []int{0, 6, 100} {
			for _, maxRows := range []int{0, 2, 16, 1000} {
				p := streamTestPartition(codec, maxRows, makeURLRecords(n))

				dir := t.TempDir()
				pathA := filepath.Join(dir, "materialized.meguri")
				pathB := filepath.Join(dir, "streamed.meguri")

				if err := EncodeToFile(pathA, p); err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: EncodeToFile: %v", codec, n, maxRows, err)
				}
				if err := StreamEncodeToFile(pathB, &sliceURLSource{recs: p.URLs}, maxRows, p, dir); err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: StreamEncodeToFile: %v", codec, n, maxRows, err)
				}

				a, err := os.ReadFile(pathA)
				if err != nil {
					t.Fatal(err)
				}
				b, err := os.ReadFile(pathB)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(a, b) {
					t.Fatalf("codec=%d n=%d maxRows=%d: streamed file differs (%d vs %d bytes)", codec, n, maxRows, len(a), len(b))
				}

				// And the streamed file decodes, to the same partition the materialized
				// file decodes to. (MaxPageRows is an encode-time hint not stored in the
				// file, so neither decoded partition carries it; comparing the two decodes
				// checks semantic equality without that artifact.)
				gotA, err := Decode(a)
				if err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: Decode materialized: %v", codec, n, maxRows, err)
				}
				gotB, err := Decode(b)
				if err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: Decode streamed: %v", codec, n, maxRows, err)
				}
				if !reflect.DeepEqual(gotA, gotB) {
					t.Fatalf("codec=%d n=%d maxRows=%d: streamed round trip mismatch", codec, n, maxRows)
				}
			}
		}
	}
}

// streamTestPartition assembles a checkpoint-shaped partition (no schedule region,
// the page cap set) for the streaming-encode gate, with two sorted host records
// and a small arena so the host and blob regions are exercised alongside the
// streamed URL table.
func streamTestPartition(codec uint8, maxRows int, urls []m.URLRecord) *Partition {
	arena := []byte{0}
	intern := func(s string) uint64 {
		off := uint64(len(arena))
		arena = append(arena, byte(len(s)))
		arena = append(arena, s...)
		return off
	}
	hosts := []m.HostRecord{
		{HostKey: m.HostKeyOf("a.org"), HostRef: intern("a.org"), Grouping: m.GroupRegistrableDomain},
		{HostKey: m.HostKeyOf("z.org"), HostRef: intern("z.org"), Grouping: m.GroupRegistrableDomain},
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostKey < hosts[j].HostKey })
	lo, hi := uint64(0), ^uint64(0)
	if len(hosts) > 0 {
		lo, hi = hosts[0].HostKey, hosts[len(hosts)-1].HostKey
	}
	return &Partition{
		ID:           9,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: 482900,
		DefaultCodec: codec,
		MaxPageRows:  maxRows,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      arena,
	}
}
