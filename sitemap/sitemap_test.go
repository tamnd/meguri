package sitemap

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
	"time"
)

// hoursOf is the test's mirror of the production conversion: a W3C datetime to
// epoch-hours. It lets a case assert the exact LastMod without hardcoding a
// magic integer.
func hoursOf(t *testing.T, s string) uint32 {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		// Fall back to date-only for cases that pass a bare date.
		parsed, err = time.Parse("2006-01-02", s)
		if err != nil {
			t.Fatalf("hoursOf: cannot parse %q: %v", s, err)
		}
	}
	return uint32(parsed.Unix() / 3600)
}

func TestParseURLSet(t *testing.T) {
	body := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/a</loc>
    <lastmod>2020-01-02T03:04:05Z</lastmod>
  </url>
  <url>
    <loc>https://example.com/b</loc>
  </url>
  <url>
    <loc>https://example.com/c</loc>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>
</urlset>`

	res, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Children) != 0 {
		t.Fatalf("urlset must not yield children, got %d", len(res.Children))
	}
	if len(res.Entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(res.Entries))
	}

	want := []Entry{
		{Loc: "https://example.com/a", LastMod: hoursOf(t, "2020-01-02T03:04:05Z"), HasLastMod: true},
		{Loc: "https://example.com/b"},
		{Loc: "https://example.com/c"},
	}
	for i, w := range want {
		got := res.Entries[i]
		if got.Loc != w.Loc {
			t.Errorf("entry %d Loc = %q, want %q", i, got.Loc, w.Loc)
		}
		if got.HasLastMod != w.HasLastMod {
			t.Errorf("entry %d HasLastMod = %v, want %v", i, got.HasLastMod, w.HasLastMod)
		}
		if got.LastMod != w.LastMod {
			t.Errorf("entry %d LastMod = %d, want %d", i, got.LastMod, w.LastMod)
		}
	}
}

func TestParseSitemapIndex(t *testing.T) {
	body := `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap-1.xml</loc>
    <lastmod>2021-06-01</lastmod>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap-2.xml.gz</loc>
  </sitemap>
</sitemapindex>`

	res, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Entries) != 0 {
		t.Fatalf("sitemapindex must not yield entries, got %d", len(res.Entries))
	}
	want := []string{
		"https://example.com/sitemap-1.xml",
		"https://example.com/sitemap-2.xml.gz",
	}
	if len(res.Children) != len(want) {
		t.Fatalf("want %d children, got %d", len(want), len(res.Children))
	}
	for i, w := range want {
		if res.Children[i] != w {
			t.Errorf("child %d = %q, want %q", i, res.Children[i], w)
		}
	}
}

func TestParseGzip(t *testing.T) {
	body := `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/gz</loc></url>
</urlset>`

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(body)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	res, err := Parse(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Parse gzip: %v", err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Loc != "https://example.com/gz" {
		t.Fatalf("gzip round-trip wrong: %+v", res.Entries)
	}
}

func TestParseTolerant(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantLen int
		wantLoc []string
	}{
		{
			name:    "entry without loc is dropped",
			body:    `<urlset><url><lastmod>2020-01-01</lastmod></url><url><loc>https://x/ok</loc></url></urlset>`,
			wantLen: 1,
			wantLoc: []string{"https://x/ok"},
		},
		{
			name:    "truncated xml keeps good entries",
			body:    `<urlset><url><loc>https://x/a</loc></url><url><loc>https://x/b</loc></ur`,
			wantLen: 1,
			wantLoc: []string{"https://x/a"},
		},
		{
			name:    "unknown elements skipped",
			body:    `<urlset><url><image>i.png</image><loc>https://x/c</loc><video>v.mp4</video></url></urlset>`,
			wantLen: 1,
			wantLoc: []string{"https://x/c"},
		},
		{
			name:    "empty body",
			body:    ``,
			wantLen: 0,
		},
		{
			name:    "loc with surrounding whitespace trimmed",
			body:    "<urlset><url><loc>\n  https://x/d  \n</loc></url></urlset>",
			wantLen: 1,
			wantLoc: []string{"https://x/d"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Parse(strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(res.Entries) != tc.wantLen {
				t.Fatalf("want %d entries, got %d (%+v)", tc.wantLen, len(res.Entries), res.Entries)
			}
			for i, w := range tc.wantLoc {
				if res.Entries[i].Loc != w {
					t.Errorf("entry %d Loc = %q, want %q", i, res.Entries[i].Loc, w)
				}
			}
		})
	}
}

func TestParseLastModFormats(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantOK     bool
		wantString string // RFC3339 or date the hours derive from, when wantOK
	}{
		{name: "date only", in: "2006-01-02", wantOK: true, wantString: "2006-01-02"},
		{name: "utc zulu", in: "2006-01-02T15:04:05Z", wantOK: true, wantString: "2006-01-02T15:04:05Z"},
		{name: "zoned offset", in: "2006-01-02T15:04:05-07:00", wantOK: true, wantString: "2006-01-02T15:04:05-07:00"},
		{name: "rfc3339 positive offset", in: "2021-12-31T23:59:59+09:00", wantOK: true, wantString: "2021-12-31T23:59:59+09:00"},
		{name: "garbage", in: "not-a-date", wantOK: false},
		{name: "empty", in: "", wantOK: false},
		{name: "pre epoch", in: "1969-01-01", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseLastMod(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("parseLastMod(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			}
			if !tc.wantOK {
				if got != 0 {
					t.Errorf("failed parse must leave 0, got %d", got)
				}
				return
			}
			parsed, err := time.Parse(time.RFC3339, tc.wantString)
			if err != nil {
				parsed, err = time.Parse("2006-01-02", tc.wantString)
				if err != nil {
					t.Fatalf("test setup parse %q: %v", tc.wantString, err)
				}
			}
			want := uint32(parsed.Unix() / 3600)
			if got != want {
				t.Errorf("parseLastMod(%q) = %d, want %d", tc.in, got, want)
			}
		})
	}
}

// TestLastModSurvivesInEntry checks the bad-date tolerance end to end: a garbage
// <lastmod> on a real entry must keep the entry but leave HasLastMod false.
func TestLastModSurvivesInEntry(t *testing.T) {
	body := `<urlset><url><loc>https://x/y</loc><lastmod>tomorrow</lastmod></url></urlset>`
	res, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(res.Entries))
	}
	e := res.Entries[0]
	if e.Loc != "https://x/y" {
		t.Errorf("Loc = %q", e.Loc)
	}
	if e.HasLastMod || e.LastMod != 0 {
		t.Errorf("bad date must leave HasLastMod=false LastMod=0, got %v %d", e.HasLastMod, e.LastMod)
	}
}

func TestMaxEntries(t *testing.T) {
	// Build a urlset with more than MaxEntries urls and confirm the parser stops
	// at the cap. We keep the count just over the cap so the test stays fast.
	var b strings.Builder
	b.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`)
	total := MaxEntries + 10
	for i := 0; i < total; i++ {
		b.WriteString(`<url><loc>https://example.com/p</loc></url>`)
	}
	b.WriteString(`</urlset>`)

	res, err := Parse(strings.NewReader(b.String()))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Entries) != MaxEntries {
		t.Fatalf("entries = %d, want cap %d", len(res.Entries), MaxEntries)
	}
}

func TestNamespacedAndCasing(t *testing.T) {
	// A document with a prefixed namespace and mixed-case tags must still parse
	// because we match on the local name lowercased.
	body := `<sm:URLSET xmlns:sm="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sm:URL><sm:LOC>https://example.com/up</sm:LOC></sm:URL>
</sm:URLSET>`
	res, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Loc != "https://example.com/up" {
		t.Fatalf("namespaced/cased parse wrong: %+v", res.Entries)
	}
}
