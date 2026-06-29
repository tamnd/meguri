package format

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestFSSTRoundTrip checks every string decodes back to itself through a trained
// table, including bytes no symbol covers, which must escape and still round-trip.
func TestFSSTRoundTrip(t *testing.T) {
	strs := [][]byte{
		[]byte("https://www.example.com/path/to/page?q=1"),
		[]byte("https://www.example.com/path/to/other"),
		[]byte("http://other.example.org/"),
		[]byte("https://www.example.com/\x00\x01\xff binary"),
		[]byte(""),
		[]byte("a"),
		[]byte("https://例え.jp/日本語"),
	}
	tbl := trainFSST(strs)
	for _, s := range strs {
		got := tbl.decode(tbl.encode(s))
		if string(got) != string(s) {
			t.Fatalf("round-trip mismatch: %q -> %q", s, got)
		}
	}
}

// TestFSSTArenaRandomAccess checks each string is decodable on its own from its
// span offset, the per-ref random access that is the whole point of FSST over the
// single-block zstd blob.
func TestFSSTArenaRandomAccess(t *testing.T) {
	strs := [][]byte{
		[]byte("https://a.example/one"),
		[]byte("https://a.example/two"),
		[]byte("https://b.example/three"),
	}
	arena, offs := BuildFSSTArena(strs)
	for i, s := range strs {
		if got := arena.Read(offs[i]); string(got) != string(s) {
			t.Fatalf("span %d: read %q, want %q", i, got, s)
		}
	}
	// A reload from the serialized regions resolves the same spans, the read side a
	// file uses after decoding the two regions off disk.
	table, spans := arena.Bytes()
	reloaded := LoadFSSTArena(table, spans)
	for i, s := range strs {
		if got := reloaded.Read(offs[i]); string(got) != string(s) {
			t.Fatalf("reloaded span %d: read %q, want %q", i, got, s)
		}
	}
	if got := arena.Read(0); got != nil {
		t.Fatalf("zero offset read %q, want nil sentinel", got)
	}
}

// TestFSSTTableSerialization checks the table survives a serialize/parse cycle so
// a reader rebuilds the exact symbols a writer trained.
func TestFSSTTableSerialization(t *testing.T) {
	tbl := trainFSST([][]byte{
		[]byte("https://www.example.com/a"),
		[]byte("https://www.example.com/b"),
	})
	parsed, _ := decodeTable(encodeTable(tbl))
	if len(parsed.syms) != len(tbl.syms) {
		t.Fatalf("symbol count %d, want %d", len(parsed.syms), len(tbl.syms))
	}
	for i := range tbl.syms {
		if string(parsed.syms[i].bytes()) != string(tbl.syms[i].bytes()) {
			t.Fatalf("symbol %d: %q, want %q", i, parsed.syms[i].bytes(), tbl.syms[i].bytes())
		}
	}
}

// TestFSSTArenaOnCorpus trains over the real URL strings and checks the arena
// round-trips every one and lands in the per-url size band doc 10 section 7 targets
// (~12-20 bytes/url), measured on real data. It skips when no corpus is configured.
func TestFSSTArenaOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	urls := loadCorpusURLStrings(t, path)
	if len(urls) < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000 to train a table", len(urls))
	}

	arena, offs := BuildFSSTArena(urls)
	for i, s := range urls {
		if got := arena.Read(offs[i]); string(got) != string(s) {
			t.Fatalf("corpus url %d did not round-trip through the arena", i)
		}
	}
	table, spans := arena.Bytes()
	var raw int
	for _, u := range urls {
		raw += len(u)
	}
	total := len(table) + len(spans)
	perURL := float64(total) / float64(len(urls))
	t.Logf("FSST per-ref: %d urls, raw %d bytes (%.1f/url), arena %d bytes (%.2f/url, table %d), random-access decodable",
		len(urls), raw, float64(raw)/float64(len(urls)), total, perURL, len(table))
	if perURL > 40 {
		t.Fatalf("FSST arena %.2f bytes/url, well above the doc's band, table is not training", perURL)
	}
}

// loadCorpusURLStrings reads the canonical URL strings out of the corpus, the input
// the string region compresses.
func loadCorpusURLStrings(tb testing.TB, path string) [][]byte {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type rec struct {
		URL string `json:"url"`
	}
	var urls [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r rec
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		urls = append(urls, []byte(r.URL))
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return urls
}
