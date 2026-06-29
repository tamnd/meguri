package robots

import (
	"strings"
	"testing"
	"time"
)

// TestWorkedExample is the RFC 9309 longest-match scenario from the package
// brief. The Allow on /private/public/ outranks the shorter Disallows, and the
// $ anchor keeps /*.pdf$ from matching a path with a tail after .pdf.
func TestWorkedExample(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: /private/
Allow: /private/public/
Disallow: /*.pdf$
`)
	r := Parse(body, "meguri/1.0")
	cases := []struct {
		path string
		want bool
	}{
		{"/private/public/report.pdf", true},
		{"/private/secret.txt", false},
		{"/notes.pdf", false},
		{"/public/report.pdfx", true},
	}
	for _, c := range cases {
		if got := r.Allowed(c.path); got != c.want {
			t.Errorf("Allowed(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestGroupSelection(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: /

User-agent: meguri
Disallow: /admin/
Allow: /
`)
	// The meguri group is more specific than *, so it should win for our agent.
	r := Parse(body, "meguri/1.0")
	if !r.Allowed("/index.html") {
		t.Errorf("meguri group should allow /index.html")
	}
	if r.Allowed("/admin/panel") {
		t.Errorf("meguri group should disallow /admin/panel")
	}

	// A different agent falls back to the * group, which blocks everything.
	other := Parse(body, "othercrawler/2.0")
	if other.Allowed("/index.html") {
		t.Errorf("other agent should fall back to * group and be blocked")
	}
}

func TestMostSpecificPrefixWins(t *testing.T) {
	body := []byte(`User-agent: meg
Disallow: /

User-agent: meguri
Allow: /
`)
	// Both "meg" and "meguri" are prefixes of the agent token; the longer one
	// is the more specific group and decides.
	r := Parse(body, "meguri/1.0")
	if !r.Allowed("/page") {
		t.Errorf("longer prefix group meguri should win and allow /page")
	}
}

func TestNoMatchingGroupAllowsAll(t *testing.T) {
	body := []byte(`User-agent: googlebot
Disallow: /
`)
	// No meguri group and no * group, so it is allow-all and Parse returns nil.
	r := Parse(body, "meguri/1.0")
	if r != nil {
		t.Errorf("expected nil Rules when no group matches and no * group")
	}
	if !r.Allowed("/anything") {
		t.Errorf("nil Rules must allow everything")
	}
}

func TestEmptyDisallowAllowsAll(t *testing.T) {
	body := []byte(`User-agent: *
Disallow:
`)
	// An empty Disallow value disallows nothing. This is the classic naive
	// parser bug, so guard it explicitly.
	r := Parse(body, "meguri/1.0")
	if !r.Allowed("/") {
		t.Errorf("empty Disallow must not block root")
	}
	if !r.Allowed("/deep/path") {
		t.Errorf("empty Disallow must not block any path")
	}
}

func TestEmptyFileAllowsAll(t *testing.T) {
	for name, body := range map[string][]byte{
		"zero":     {},
		"blank":    []byte("\n\n  \n"),
		"comments": []byte("# just a comment\n# another\n"),
	} {
		t.Run(name, func(t *testing.T) {
			r := Parse(body, "meguri/1.0")
			if r != nil {
				t.Errorf("empty file should parse to nil Rules")
			}
			if !r.Allowed("/x") {
				t.Errorf("empty file must allow everything")
			}
		})
	}
}

func TestBOMStripped(t *testing.T) {
	body := append([]byte{0xEF, 0xBB, 0xBF}, []byte("User-agent: *\nDisallow: /no/\n")...)
	r := Parse(body, "meguri/1.0")
	if r == nil {
		t.Fatalf("BOM should be stripped and the group parsed")
	}
	if r.Allowed("/no/page") {
		t.Errorf("rule after BOM should apply")
	}
}

func TestCrawlDelay(t *testing.T) {
	cases := []struct {
		name string
		body string
		want time.Duration
	}{
		{"integer", "User-agent: *\nCrawl-delay: 5\n", 5 * time.Second},
		{"fractional", "User-agent: *\nCrawl-delay: 0.5\n", 500 * time.Millisecond},
		{"none", "User-agent: *\nDisallow: /x\n", 0},
		{"garbage", "User-agent: *\nCrawl-delay: soon\n", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Parse([]byte(c.body), "meguri/1.0")
			if got := r.CrawlDelay(); got != c.want {
				t.Errorf("CrawlDelay() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestCrawlDelayBelongsToGroup(t *testing.T) {
	body := []byte(`User-agent: *
Crawl-delay: 1

User-agent: meguri
Crawl-delay: 10
`)
	r := Parse(body, "meguri/1.0")
	if got := r.CrawlDelay(); got != 10*time.Second {
		t.Errorf("CrawlDelay() = %v, want 10s from the meguri group", got)
	}
}

func TestSitemaps(t *testing.T) {
	body := []byte(`Sitemap: https://example.com/sitemap.xml
User-agent: *
Disallow: /x/
Sitemap: https://example.com/news.xml
`)
	r := Parse(body, "meguri/1.0")
	got := r.Sitemaps()
	want := []string{
		"https://example.com/sitemap.xml",
		"https://example.com/news.xml",
	}
	if len(got) != len(want) {
		t.Fatalf("Sitemaps() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Sitemaps()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSitemapOnlyFile(t *testing.T) {
	// A file with only sitemaps and no matching group is allow-all for fetching
	// but still must expose its sitemaps.
	body := []byte("Sitemap: https://example.com/s.xml\n")
	r := Parse(body, "meguri/1.0")
	if r == nil {
		t.Fatalf("file with sitemaps should not parse to nil")
	}
	if !r.Allowed("/anything") {
		t.Errorf("sitemap-only file should allow everything")
	}
	if len(r.Sitemaps()) != 1 {
		t.Errorf("expected one sitemap, got %v", r.Sitemaps())
	}
}

func TestNilReceiver(t *testing.T) {
	var r *Rules
	if !r.Allowed("/x") {
		t.Errorf("nil receiver Allowed must be true")
	}
	if r.CrawlDelay() != 0 {
		t.Errorf("nil receiver CrawlDelay must be 0")
	}
	if r.Sitemaps() != nil {
		t.Errorf("nil receiver Sitemaps must be nil")
	}
}

func TestTieGoesToAllow(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: /page
Allow: /page
`)
	// Equal longest match between Allow and Disallow resolves to Allow.
	r := Parse(body, "meguri/1.0")
	if !r.Allowed("/page") {
		t.Errorf("equal-length Allow and Disallow must resolve to Allow")
	}
}

func TestTieGoesToAllowReverseOrder(t *testing.T) {
	body := []byte(`User-agent: *
Allow: /page
Disallow: /page
`)
	// Order in the file must not change the tie outcome.
	r := Parse(body, "meguri/1.0")
	if !r.Allowed("/page") {
		t.Errorf("tie must go to Allow regardless of line order")
	}
}

func TestTolerantParsing(t *testing.T) {
	body := []byte(`User-agent: *
Disallow /no-colon-here
Unknown-Directive: whatever
Disallow: /blocked/
Crawl-delay: not-a-number
garbage line with no structure
`)
	r := Parse(body, "meguri/1.0")
	if r == nil {
		t.Fatalf("tolerant parse should still build the group")
	}
	if r.Allowed("/blocked/x") {
		t.Errorf("the one valid Disallow should still apply")
	}
	if r.Allowed("/no-colon-here") == false {
		t.Errorf("a malformed line must not become a real rule")
	}
}

func TestWildcardMatching(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: /*/admin
Disallow: /tmp/
Disallow: /*.gif$
`)
	r := Parse(body, "meguri/1.0")
	cases := []struct {
		path string
		want bool
	}{
		{"/a/admin", false},
		{"/x/y/admin", false},
		{"/admin", true}, // needs something before /admin
		{"/tmp/file", false},
		{"/img/logo.gif", false},
		{"/img/logo.gifx", true}, // $ anchor blocks the tail
		{"/clean", true},
	}
	for _, c := range cases {
		if got := r.Allowed(c.path); got != c.want {
			t.Errorf("Allowed(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestPatternWithoutLeadingSlash(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: private
`)
	// A pattern with no leading slash is tolerated and matched as a prefix.
	r := Parse(body, "meguri/1.0")
	if r.Allowed("private/data") {
		t.Errorf("leading-slash-less pattern should still match")
	}
}

func TestCaseInsensitiveDirectivesAndAgent(t *testing.T) {
	body := []byte(`USER-AGENT: MeGuRi
DISALLOW: /no/
`)
	r := Parse(body, "meguri/1.0")
	if r == nil {
		t.Fatalf("directives and agents must match case-insensitively")
	}
	if r.Allowed("/no/x") {
		t.Errorf("uppercase Disallow should apply")
	}
}

func TestPathMatchedNotURL(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: /search?
`)
	r := Parse(body, "meguri/1.0")
	if r.Allowed("/search?q=1") {
		t.Errorf("query portion of the path should be matched")
	}
	if !r.Allowed("/results") {
		t.Errorf("unrelated path should be allowed")
	}
}

func TestMaxBytes(t *testing.T) {
	// A rule that lives past the MaxBytes cap must not be parsed. Pad with
	// comment bytes up to the cap, then place a Disallow after it.
	pad := strings.Repeat("# pad\n", (MaxBytes/6)+10)
	body := []byte("User-agent: *\n" + pad + "Disallow: /late/\n")
	if len(body) <= MaxBytes {
		t.Fatalf("test setup: body must exceed MaxBytes, got %d", len(body))
	}
	// The late rule lives beyond the cap, so /late/ must stay allowed.
	r := Parse(body, "meguri/1.0")
	if !r.Allowed("/late/x") {
		t.Errorf("rule past MaxBytes must not be parsed")
	}
}

func TestAnchorOnPlainLiteral(t *testing.T) {
	body := []byte(`User-agent: *
Disallow: /exact$
`)
	r := Parse(body, "meguri/1.0")
	if r.Allowed("/exact") {
		t.Errorf("/exact should match the anchored pattern")
	}
	if !r.Allowed("/exact/more") {
		t.Errorf("$ anchor must reject a longer path")
	}
}
