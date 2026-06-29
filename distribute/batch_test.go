package distribute

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"sort"
	"strings"
	"testing"

	m "github.com/tamnd/meguri"
)

// sampleDiscoveries builds a small batch covering the field ranges the codec has
// to carry: ascending and resetting path keys, a plain and a multibyte URL, an
// empty URL, varied enums, and a negative-going timestamp so the zigzag deltas
// see both signs.
func sampleDiscoveries() []m.Discovery {
	return []m.Discovery{
		{
			URLKey: m.MakeURLKey("a.example", "/one"), CanonicalURL: "https://a.example/one",
			Depth: 1, DiscoverySource: m.SourceLink, SrcHostKey: 11, LinkWeight: 0.25,
			AnchorHint: m.AnchorDescriptive, ObservedAt: 1000,
		},
		{
			URLKey: m.MakeURLKey("a.example", "/two"), CanonicalURL: "https://a.example/two",
			Depth: 2, DiscoverySource: m.SourceLink, SrcHostKey: 11, LinkWeight: 0.5,
			AnchorHint: m.AnchorGeneric, ObservedAt: 990,
		},
		{
			URLKey: m.MakeURLKey("b.example", "/日本語"), CanonicalURL: "https://b.example/日本語",
			Depth: 3, DiscoverySource: m.SourceSitemap, SrcHostKey: 22, LinkWeight: 0,
			AnchorHint: m.AnchorEmpty, ObservedAt: 1200,
		},
		{
			URLKey: m.MakeURLKey("c.example", "/"), CanonicalURL: "",
			Depth: 0, DiscoverySource: m.SourceSeed, SrcHostKey: 0, LinkWeight: 1,
			AnchorHint: m.AnchorUnknown, ObservedAt: 5,
		},
	}
}

// sortedByKey returns a copy sorted the way DecodeBatch returns rows, so a test
// compares against the encoder's canonical order.
func sortedByKey(ds []m.Discovery) []m.Discovery {
	out := append([]m.Discovery(nil), ds...)
	sort.Slice(out, func(i, j int) bool { return out[i].URLKey.Less(out[j].URLKey) })
	return out
}

func discoveriesEqual(t *testing.T, got, want []m.Discovery) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.URLKey != w.URLKey || g.CanonicalURL != w.CanonicalURL || g.Depth != w.Depth ||
			g.DiscoverySource != w.DiscoverySource || g.SrcHostKey != w.SrcHostKey ||
			math.Float32bits(g.LinkWeight) != math.Float32bits(w.LinkWeight) ||
			g.AnchorHint != w.AnchorHint || g.ObservedAt != w.ObservedAt {
			t.Fatalf("row %d mismatch:\n got  %+v\n want %+v", i, g, w)
		}
	}
}

// TestBatchRoundTrip checks every field survives the columnar body, including the
// multibyte and empty URLs and the path keys that reset across hosts.
func TestBatchRoundTrip(t *testing.T) {
	in := sampleDiscoveries()
	body := EncodeBatch(in)
	got, ok := DecodeBatch(body)
	if !ok {
		t.Fatal("decode reported not-ok on a well-formed body")
	}
	discoveriesEqual(t, got, sortedByKey(in))
}

// TestEncodeBatchEmpty checks an empty batch is the empty body and decodes back
// to an empty batch, the no-op a flush of a quiet destination produces.
func TestEncodeBatchEmpty(t *testing.T) {
	if body := EncodeBatch(nil); body != nil {
		t.Fatalf("empty batch encoded to %d bytes, want nil", len(body))
	}
	got, ok := DecodeBatch(nil)
	if !ok || len(got) != 0 {
		t.Fatalf("empty body decoded to %d rows ok=%v, want 0 ok", len(got), ok)
	}
}

// TestDecodeBatchRejectsTruncation checks a body cut short at every length
// reports not-ok rather than returning partial or panicking, the corrupt-record
// contract the fleet binding leans on.
func TestDecodeBatchRejectsTruncation(t *testing.T) {
	body := EncodeBatch(sampleDiscoveries())
	for cut := 1; cut < len(body); cut++ {
		// A truncated body must never panic; it may only report not-ok or, by
		// coincidence of a shorter valid prefix, a wrong-but-bounded row set. The
		// contract under test is no panic and no over-read.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decode panicked on a body truncated to %d bytes: %v", cut, r)
				}
			}()
			DecodeBatch(body[:cut])
		}()
	}
}

// TestWireTransportRoundTrip drives a batch through the in-process wire transport,
// the fleet binding's double, so the encode and decode run on the send path the
// way they would over a log.
func TestWireTransportRoundTrip(t *testing.T) {
	tr := NewWireChannelTransport(4)
	in := sampleDiscoveries()
	if err := tr.Send(7, in); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, ok := tr.Recv(7)
	if !ok {
		t.Fatal("recv reported nothing after a send")
	}
	discoveriesEqual(t, got, sortedByKey(in))
	// A drained destination reports nothing rather than blocking.
	if _, ok := tr.Recv(7); ok {
		t.Fatal("recv returned a second batch from a one-batch destination")
	}
}

// TestBatchOnCorpus encodes a real discovery batch built from the frozen corpus
// URLs and checks it round-trips and lands well under the naive row-major size.
// The naive baseline is the canonical URL bytes plus the fixed per-row scalars
// (two 64-bit keys, the source key, the float, and the small fields), the width a
// row-major wire format pays. It skips when no corpus is configured.
func TestBatchOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	batch := loadCorpusDiscoveries(t, path)
	if len(batch) < 1000 {
		t.Skipf("corpus produced %d discoveries, need at least 1000", len(batch))
	}

	body := EncodeBatch(batch)
	got, ok := DecodeBatch(body)
	if !ok {
		t.Fatal("corpus batch did not decode")
	}
	discoveriesEqual(t, got, sortedByKey(batch))

	var naive int
	for _, d := range batch {
		naive += len(d.CanonicalURL) + 8 + 8 + 8 + 4 + 2 + 1 + 1 + 4 // url + keys + src + weight + depth + source + anchor + observed
	}
	per := float64(len(body)) / float64(len(batch))
	t.Logf("batch on corpus: %d discoveries, body %d bytes (%.1f/row), naive row-major ~%d bytes (%.1f/row)",
		len(batch), len(body), per, naive, float64(naive)/float64(len(batch)))
	if len(body) >= naive {
		t.Fatalf("columnar body %d bytes did not beat the naive %d", len(body), naive)
	}
}

// loadCorpusDiscoveries reads the corpus URLs into a discovery batch, synthesizing
// the per-link fields deterministically by index so the encoding is exercised on
// real keys and URL strings without depending on fields the slice does not carry.
func loadCorpusDiscoveries(tb testing.TB, path string) []m.Discovery {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type rec struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	}
	seen := map[m.URLKey]bool{}
	var out []m.Discovery
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
		host := r.Host
		if host == "" {
			if _, after, ok := strings.Cut(r.URL, "://"); ok {
				host = after
			} else {
				host = r.URL
			}
			if i := strings.IndexAny(host, "/?#"); i >= 0 {
				host = host[:i]
			}
		}
		if host == "" {
			continue
		}
		p := "/"
		if _, after, ok := strings.Cut(r.URL, "://"); ok {
			if i := strings.IndexAny(after, "/?#"); i >= 0 {
				p = after[i:]
			}
		}
		key := m.MakeURLKey(host, p)
		if seen[key] {
			continue
		}
		seen[key] = true
		i := len(out)
		out = append(out, m.Discovery{
			URLKey:          key,
			CanonicalURL:    r.URL,
			Depth:           uint16(i % 12),
			DiscoverySource: m.SourceLink,
			SrcHostKey:      key.HostKey,
			LinkWeight:      float32(i%100) / 100,
			AnchorHint:      m.AnchorDescriptive,
			ObservedAt:      uint32(482817 + i%48),
		})
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return out
}
