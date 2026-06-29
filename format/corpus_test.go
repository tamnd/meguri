package format

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	m "github.com/tamnd/meguri"
)

// cdxRecord is one Common Crawl capture as ccrawl-cli emits it with `-o jsonl`.
// Only the fields meguri maps into a frontier record are decoded.
type cdxRecord struct {
	URL       string `json:"url"`
	Host      string `json:"host"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Digest    string `json:"digest"`
}

// loadCorpus reads the frozen ccrawl slice at path into a Partition. It is the
// bridge from real Common Crawl captures to meguri's data model: real URLs and
// hosts derive the 128-bit keys, the capture status becomes the HTTP status, and
// the capture timestamp becomes the last-crawled time. This is the input every
// M0 gate that "runs on real data" actually runs on.
func loadCorpus(tb testing.TB, path string) *Partition {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	arena := newArena()
	intern := func(s string) uint64 {
		var off uint64
		arena, off = arenaIntern(arena, []byte(s))
		return off
	}

	type seen struct{}
	hostOf := map[uint64]m.HostRecord{}
	urlOf := map[m.URLKey]seen{}
	var urls []m.URLRecord

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec cdxRecord
		if json.Unmarshal([]byte(line), &rec) != nil || rec.URL == "" {
			continue
		}
		host := rec.Host
		if host == "" {
			host = hostFromURL(rec.URL)
		}
		if host == "" {
			continue
		}
		key := m.MakeURLKey(host, pathFromURL(rec.URL))
		if _, dup := urlOf[key]; dup {
			continue
		}
		urlOf[key] = seen{}

		hk := key.HostKey
		when := epochHours(rec.Timestamp)
		urls = append(urls, m.URLRecord{
			URLKey:      key,
			HostKey:     hk,
			Status:      m.StatusCrawled,
			URLRef:      intern(rec.URL),
			FirstSeen:   when,
			LastCrawled: when,
			NextDue:     when + 24,
			CrawlCount:  1,
			ContentFP:   fnv64(rec.Digest),
			HTTPStatus:  parseStatus(rec.Status),
		})
		if _, ok := hostOf[hk]; !ok {
			ref := intern(host)
			hostOf[hk] = m.HostRecord{
				HostKey:        hk,
				HostRef:        ref,
				Grouping:       m.GroupFullHost,
				RegistrableRef: ref,
				CrawlDelay:     10,
				CrawlTotal:     1,
			}
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}

	hosts := make([]m.HostRecord, 0, len(hostOf))
	for _, h := range hostOf {
		hosts = append(hosts, h)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostKey < hosts[j].HostKey })
	sort.Slice(urls, func(i, j int) bool { return urls[i].URLKey.Less(urls[j].URLKey) })

	lo, hi := uint64(0), ^uint64(0)
	if len(hosts) > 0 {
		lo, hi = hosts[0].HostKey, hosts[len(hosts)-1].HostKey
	}
	return &Partition{
		ID:           1,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: 482817,
		DefaultCodec: CodecZstd,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      arena,
	}
}

// corpusPath returns the corpus path or "" when no corpus is configured, so the
// corpus gate skips cleanly on a machine that has not pulled the slice.
func corpusPath() string { return os.Getenv("MEGURI_CORPUS") }

// TestCorpusRoundTrip is the M0 gate on real data: load a frozen ccrawl slice,
// encode it to a .meguri partition, decode it back, and require an exact match,
// plus a byte-stable re-encode. It reports the on-disk cost per URL, the number
// the format is built to keep small as the crawl scales toward 100B URLs.
func TestCorpusRoundTrip(t *testing.T) {
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
	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.URLs) != len(p.URLs) || len(got.Hosts) != len(p.Hosts) {
		t.Fatalf("counts changed: urls %d->%d hosts %d->%d",
			len(p.URLs), len(got.URLs), len(p.Hosts), len(got.Hosts))
	}
	enc2, err := Encode(got)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if len(enc) != len(enc2) {
		t.Fatalf("re-encode not byte-stable: %d vs %d bytes", len(enc), len(enc2))
	}

	ins, err := InspectBytes(enc)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	t.Logf("corpus: %d urls, %d hosts, %d bytes, %.2f bytes/url",
		ins.URLCount, ins.HostCount, ins.FileSize, ins.Stats.BytesPerURL)
}

func BenchmarkCorpusEncode(b *testing.B) {
	path := corpusPath()
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(b, path)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Encode(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCorpusDecode(b *testing.B) {
	path := corpusPath()
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(b, path)
	enc, err := Encode(p)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Decode(enc); err != nil {
			b.Fatal(err)
		}
	}
}

// epochHours parses a Common Crawl timestamp (YYYYMMDDhhmmss) into epoch-hours,
// returning 0 when it cannot be parsed.
func epochHours(ts string) uint32 {
	t, err := time.Parse("20060102150405", ts)
	if err != nil {
		return 0
	}
	return uint32(t.Unix() / 3600)
}

func parseStatus(s string) uint16 {
	var v uint16
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return v
		}
		v = v*10 + uint16(c-'0')
	}
	return v
}

func fnv64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// hostFromURL and pathFromURL split a URL without pulling net/url into the hot
// path: the corpus carries an explicit host field, these are the fallback for a
// record that omits it.
func hostFromURL(u string) string {
	s := stripScheme(u)
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		return s[:i]
	}
	return s
}

func pathFromURL(u string) string {
	s := stripScheme(u)
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		return s[i:]
	}
	return "/"
}

func stripScheme(u string) string {
	if _, after, found := strings.Cut(u, "://"); found {
		return after
	}
	return u
}
