package robots

import "testing"

// a realistic robots.txt: a couple of groups, a handful of rules each, the kind
// of file a host serves and the parser runs once per host per cache window.
var benchBody = []byte(`User-agent: *
Disallow: /search
Disallow: /*?
Allow: /search/about
Crawl-delay: 1

User-agent: meguri
Disallow: /private/
Allow: /private/public
Disallow: /tmp/

Sitemap: https://example.com/sitemap.xml
`)

// BenchmarkParse measures parsing a host's robots.txt into matchable rules, the
// cost paid once per host per 24h cache window.
func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = Parse(benchBody, "meguri")
	}
}

// BenchmarkAllowed measures the per-URL allow check, the hot path: at 100B pages
// every discovered URL is tested against its host's rules, so this runs once per
// URL and has to stay cheap.
func BenchmarkAllowed(b *testing.B) {
	r := Parse(benchBody, "meguri")
	paths := []string{"/private/public/doc", "/articles/2026/a", "/private/secret", "/tmp/x"}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = r.Allowed(paths[i%len(paths)])
		i++
	}
}
