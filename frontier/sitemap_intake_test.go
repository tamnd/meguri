package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
	"github.com/tamnd/meguri/sitemap"
)

// TestSitemapDiscoveriesEnterFrontier is the end-to-end intake gate: a parsed
// sitemap becomes discoveries (sitemap.Result.Discoveries), those discoveries fold
// into a live frontier through Discover, and the lastmod prior actually lands as the
// record's FirstSeen so the freshness consumer reads it. A URL with a parseable
// lastmod enters with FirstSeen at that hour; one without enters observed now. Both
// are scheduled, deduped, and stamped SourceSitemap.
func TestSitemapDiscoveriesEnterFrontier(t *testing.T) {
	const now = uint32(2_000_000)
	res := &sitemap.Result{Entries: []sitemap.Entry{
		{Loc: "https://news.example/old", LastMod: 100_000, HasLastMod: true},
		{Loc: "https://news.example/new", HasLastMod: false},
	}}
	pol := &dedup.CanonPolicy{}
	discs := res.Discoveries("https://news.example/sitemap.xml", meguri.GroupRegistrableDomain, pol, now)
	if len(discs) != 2 {
		t.Fatalf("intake produced %d discoveries, want 2", len(discs))
	}

	f := New(1, 0)
	admitted := 0
	for _, d := range discs {
		if f.Discover(d, now) {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("admitted %d sitemap URLs, want 2 scheduled", admitted)
	}

	for _, d := range discs {
		rec := f.records[d.URLKey]
		if rec == nil {
			t.Fatalf("discovery %q did not create a record", d.CanonicalURL)
		}
		if rec.DiscoverySource != meguri.SourceSitemap {
			t.Fatalf("record %q source = %v, want SourceSitemap", d.CanonicalURL, rec.DiscoverySource)
		}
		if rec.FirstSeen != d.ObservedAt {
			t.Fatalf("record %q FirstSeen = %d, want the discovery's ObservedAt %d (the lastmod prior must land)", d.CanonicalURL, rec.FirstSeen, d.ObservedAt)
		}
	}

	// The lastmod-bearing URL carries the old prior; the bare one carries now. The
	// prior is the weak freshness signal, distinct between the two.
	oldKey, _, _, _ := dedup.CanonicalKey("https://news.example/old", "", meguri.GroupRegistrableDomain, pol)
	if got := f.records[oldKey].FirstSeen; got != 100_000 {
		t.Fatalf("lastmod URL FirstSeen = %d, want 100000", got)
	}
	newKey, _, _, _ := dedup.CanonicalKey("https://news.example/new", "", meguri.GroupRegistrableDomain, pol)
	if got := f.records[newKey].FirstSeen; got != now {
		t.Fatalf("no-lastmod URL FirstSeen = %d, want now=%d", got, now)
	}

	// Redelivery dedups: the same discoveries a second time admit nothing new.
	for _, d := range discs {
		if f.Discover(d, now) {
			t.Fatalf("redelivered sitemap discovery %q admitted a second record", d.CanonicalURL)
		}
	}
}
