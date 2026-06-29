package dedup

import (
	"testing"

	"github.com/tamnd/meguri"
)

// TestCanonicalCollapse is doc 03's worked example: five raw URLs of the kind a
// crawl finds in the wild all canonicalize to one form, so all five are one
// frontier entry. This pins the collapse half of the asymmetry rule.
func TestCanonicalCollapse(t *testing.T) {
	const want = "http://example.com/a/c?id=7"
	raws := []string{
		"HTTP://Example.COM:80/a/b/../c?utm_source=news&id=7#section",
		"http://example.com/a/c?id=7&utm_source=news",
		"http://example.com/a/c?id=7",
		"http://example.com/%61/c?id=7",
		"http://EXAMPLE.com/a/c/../c?id=7#",
	}
	for _, raw := range raws {
		got, ok := Canonicalize(raw, "", nil)
		if !ok {
			t.Fatalf("Canonicalize(%q) dropped a valid URL", raw)
		}
		if got != want {
			t.Errorf("Canonicalize(%q) = %q, want %q", raw, got, want)
		}
	}

	// All five must key to one URLKey.
	first, _, ok := Key(want, meguri.GroupRegistrableDomain)
	if !ok {
		t.Fatal("Key on canonical form failed")
	}
	for _, raw := range raws {
		k, _, _, ok := CanonicalKey(raw, "", meguri.GroupRegistrableDomain, nil)
		if !ok || k != first {
			t.Errorf("CanonicalKey(%q) = %v, want %v", raw, k, first)
		}
	}
}

// TestCanonicalKeepDistinct is the flip side: the variants that must stay
// distinct under the generic rules, because each can name a different resource on
// some server and a wrong merge silently drops a page (doc 03).
func TestCanonicalKeepDistinct(t *testing.T) {
	pairs := [][2]string{
		{"http://example.com/a/c?id=7", "http://example.com/a/c?id=8"},
		{"http://example.com/a/c", "http://example.com/a/c/"},
		{"http://example.com/a/c", "http://example.com/A/C"},
		{"http://example.com:8080/a/c", "http://example.com/a/c"},
		{"https://example.com/a/c", "http://example.com/a/c"},
		{"http://example.com/a%2Fc", "http://example.com/a/c"},
	}
	for _, p := range pairs {
		a, ok1 := Canonicalize(p[0], "", nil)
		b, ok2 := Canonicalize(p[1], "", nil)
		if !ok1 || !ok2 {
			t.Fatalf("dropped a valid URL: %q %q", p[0], p[1])
		}
		if a == b {
			t.Errorf("wrongly collapsed %q and %q both to %q", p[0], p[1], a)
		}
	}
}

// TestCanonicalDropsNonFrontier checks that links which do not resolve to an
// absolute http(s) URL are dropped, not canonicalized (doc 03, step 1).
func TestCanonicalDropsNonFrontier(t *testing.T) {
	for _, raw := range []string{
		"mailto:a@example.com",
		"javascript:void(0)",
		"tel:+1234",
		"data:text/plain,hi",
		"ftp://example.com/x",
		"/relative/only", // no base to resolve against
	} {
		if got, ok := Canonicalize(raw, "", nil); ok {
			t.Errorf("Canonicalize(%q) should drop, got %q", raw, got)
		}
	}
}

// TestCanonicalRelative resolves a relative link against the page it was found
// on, the first canonicalization step.
func TestCanonicalRelative(t *testing.T) {
	got, ok := Canonicalize("../c?id=7", "http://example.com/a/b/page.html", nil)
	if !ok {
		t.Fatal("relative resolution dropped a valid URL")
	}
	if want := "http://example.com/a/c?id=7"; got != want {
		t.Errorf("relative = %q, want %q", got, want)
	}
}

// TestCanonicalIDN folds a non-ASCII host to punycode so the unicode and the
// already-encoded forms are one host (doc 03, step 3).
func TestCanonicalIDN(t *testing.T) {
	a, ok1 := Canonicalize("http://münchen.de/", "", nil)
	b, ok2 := Canonicalize("http://xn--mnchen-3ya.de/", "", nil)
	if !ok1 || !ok2 {
		t.Fatal("IDN host dropped")
	}
	if a != b {
		t.Errorf("IDN forms not collapsed: %q vs %q", a, b)
	}
}

// TestQuerySortAndFilter checks the tracking deny-list and the stable sort.
func TestQuerySortAndFilter(t *testing.T) {
	got, ok := Canonicalize("http://e.com/p?b=2&a=1&utm_campaign=x&fbclid=y&a=0", "", nil)
	if !ok {
		t.Fatal("dropped")
	}
	if want := "http://e.com/p?a=0&a=1&b=2"; got != want {
		t.Errorf("query filter/sort = %q, want %q", got, want)
	}
}

// TestPolicyFolding exercises the per-host folding opt-ins: a host configured to
// fold trailing slashes and index files collapses the variants the generic rules
// keep distinct (doc 03, steps 8, 9).
func TestPolicyFolding(t *testing.T) {
	pol := &CanonPolicy{FoldTrailing: true, FoldIndex: true, Version: 1}
	dir, _ := Canonicalize("http://e.com/dir/", "", pol)
	idx, _ := Canonicalize("http://e.com/dir/index.html", "", pol)
	if dir != idx {
		t.Errorf("FoldIndex did not collapse: %q vs %q", dir, idx)
	}
	a, _ := Canonicalize("http://e.com/about/", "", pol)
	if a != "http://e.com/about" {
		t.Errorf("FoldTrailing = %q, want http://e.com/about", a)
	}
}

// TestPolicyAllowlist checks allowlist mode: only the named keys survive.
func TestPolicyAllowlist(t *testing.T) {
	pol := &CanonPolicy{QueryAllow: []string{"id"}, Version: 1}
	got, _ := Canonicalize("http://e.com/p?id=7&color=red&size=xl", "", pol)
	if want := "http://e.com/p?id=7"; got != want {
		t.Errorf("allowlist = %q, want %q", got, want)
	}
}

// TestRegistrableDomain pins the PSL+1 grouping examples from doc 03, including
// the public-suffix entries where each subdomain is its own registrable domain.
func TestRegistrableDomain(t *testing.T) {
	cases := map[string]string{
		"www.example.com":      "example.com",
		"a.b.example.com":      "example.com",
		"example.co.uk":        "example.co.uk",
		"shop.example.co.uk":   "example.co.uk",
		"user.github.io":       "user.github.io",
		"foo.s3.amazonaws.com": "foo.s3.amazonaws.com",
	}
	for host, want := range cases {
		if got := RegistrableDomain(host); got != want {
			t.Errorf("RegistrableDomain(%q) = %q, want %q", host, got, want)
		}
	}
}

// TestGroupingCollapsesSubdomains checks the politeness consequence: two
// subdomains of one registrable domain share a HostKey (one politeness group) but
// differ in PathKey, so they colocate and are polite together while staying
// distinct URLs (doc 03).
func TestGroupingCollapsesSubdomains(t *testing.T) {
	a, _, _, _ := CanonicalKey("http://a.example.com/p", "", meguri.GroupRegistrableDomain, nil)
	b, _, _, _ := CanonicalKey("http://b.example.com/p", "", meguri.GroupRegistrableDomain, nil)
	if a.HostKey != b.HostKey {
		t.Errorf("subdomains did not share a HostKey: %x vs %x", a.HostKey, b.HostKey)
	}
	if a.PathKey == b.PathKey {
		t.Error("distinct subdomains collapsed to one PathKey")
	}
	// Under the full-host override they are separate groups.
	c, _, _, _ := CanonicalKey("http://a.example.com/p", "", meguri.GroupFullHost, nil)
	d, _, _, _ := CanonicalKey("http://b.example.com/p", "", meguri.GroupFullHost, nil)
	if c.HostKey == d.HostKey {
		t.Error("full-host override did not separate subdomains")
	}
}
