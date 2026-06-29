package sitemap

import (
	"strings"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// TestDiscoveriesMapEntries gates the intake mapping: each urlset entry becomes a
// discovery stamped SourceSitemap and keyed off its canonical URL, lastmod rides in
// as the ObservedAt freshness prior when present and falls back to now when absent,
// and an entry whose loc is not a frontier URL is dropped rather than admitted with
// a bad key.
func TestDiscoveriesMapEntries(t *testing.T) {
	const now = uint32(1_000_000)
	res := &Result{Entries: []Entry{
		{Loc: "https://shop.example/a", LastMod: 900_000, HasLastMod: true},
		{Loc: "/b", LastMod: 0, HasLastMod: false},           // relative, resolves against base
		{Loc: "javascript:void(0)"},                          // not a frontier URL
		{Loc: "https://shop.example/c?utm_source=news#frag"}, // tracking + fragment stripped by canon
	}}

	pol := &dedup.CanonPolicy{}
	got := res.Discoveries("https://shop.example/sitemap.xml", meguri.GroupRegistrableDomain, pol, now)
	if len(got) != 3 {
		t.Fatalf("got %d discoveries, want 3 (the javascript loc dropped)", len(got))
	}

	for _, d := range got {
		if d.DiscoverySource != meguri.SourceSitemap {
			t.Fatalf("discovery %q source = %v, want SourceSitemap", d.CanonicalURL, d.DiscoverySource)
		}
		if d.SrcHostKey != meguri.HostKeyOf("shop.example") {
			t.Fatalf("discovery %q src host = %#x, want the sitemap host", d.CanonicalURL, d.SrcHostKey)
		}
		wantKey, _, _, ok := dedup.CanonicalKey(d.CanonicalURL, "", meguri.GroupRegistrableDomain, pol)
		if !ok || d.URLKey != wantKey {
			t.Fatalf("discovery %q key %v does not match its canonical URL key %v", d.CanonicalURL, d.URLKey, wantKey)
		}
	}

	// The first entry's parseable lastmod is the weak prior; the relative entry had
	// none and is observed now.
	byURL := map[string]meguri.Discovery{}
	for _, d := range got {
		byURL[d.CanonicalURL] = d
	}
	if a := byURL["https://shop.example/a"]; a.ObservedAt != 900_000 {
		t.Fatalf("lastmod entry observed at %d, want the lastmod 900000", a.ObservedAt)
	}
	if b, ok := byURL["https://shop.example/b"]; !ok || b.ObservedAt != now {
		t.Fatalf("no-lastmod entry observed at %d (present=%v), want now=%d", b.ObservedAt, ok, now)
	}
	// The tracking param and fragment never reach the key: the canonical URL is clean.
	if c, ok := byURL["https://shop.example/c"]; !ok {
		t.Fatalf("tracking-param entry did not canonicalize to a clean URL: %v", c)
	}
}

// TestChildURLsResolve gates the sitemapindex side: child sitemap locations
// resolve against base and come back as fetchable URLs, carrying no discovery
// because a child is a sitemap to fetch, not a page to crawl.
func TestChildURLsResolve(t *testing.T) {
	res := &Result{Children: []string{
		"https://shop.example/sitemap-1.xml",
		"/sitemap-2.xml", // relative, resolves against base
		"mailto:a@b.com", // non-http scheme, dropped
	}}
	got := res.ChildURLs("https://shop.example/sitemap.xml", &dedup.CanonPolicy{})
	if len(got) != 2 {
		t.Fatalf("got %d child URLs, want 2", len(got))
	}
	for _, u := range got {
		if !strings.HasPrefix(u, "https://shop.example/sitemap-") {
			t.Fatalf("child URL %q did not resolve against base", u)
		}
	}
}

// TestDiscoveriesEmpty checks an index result (no urlset entries) yields no
// discoveries: the two sides of a sitemap never cross.
func TestDiscoveriesEmpty(t *testing.T) {
	res := &Result{Children: []string{"https://shop.example/sitemap-1.xml"}}
	if got := res.Discoveries("https://shop.example/sitemap.xml", meguri.GroupRegistrableDomain, &dedup.CanonPolicy{}, 5); got != nil {
		t.Fatalf("a sitemapindex produced %d page discoveries, want none", len(got))
	}
}
