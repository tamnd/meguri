package sitemap

import (
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/dedup"
)

// Discoveries turns a parsed urlset into the frontier discoveries it feeds, the
// intake half of the sitemap path: the parser reads <loc> and <lastmod>, and this
// maps each surviving entry onto a meguri.Discovery the frontier's Discover folds
// in. It is the freshness/prioritization consumer the audit names, kept on the
// meguri side because canonicalization and the URLKey must be trusted before a URL
// enters the frontier (invariant 10); ami fetches the sitemap bytes and hands them
// to Parse, this turns the result into routable discoveries.
//
// Each entry's Loc is canonicalized against base (the sitemap's own URL, so a
// relative <loc> resolves) and keyed; an entry whose Loc is not a frontier URL is
// dropped rather than admitted with a bad key. The discovery is stamped with
// meguri.SourceSitemap so the record records how it entered.
//
// lastmod rides in as the weak freshness prior, the only sitemap-declared signal
// meguri trusts: a parseable <lastmod> becomes the discovery's ObservedAt, which
// the frontier stamps as the record's FirstSeen, so a never-crawled URL enters with
// a prior on its age (an old lastmod reads as a long-lived, stable page; a recent
// one as fresh) that the change-rate estimator starts from. When lastmod is absent
// the discovery is observed now. changefreq and priority are never consulted, the
// same refusal the parser makes.
func (res *Result) Discoveries(base string, g meguri.HostGrouping, pol *dedup.CanonPolicy, now uint32) []meguri.Discovery {
	if len(res.Entries) == 0 {
		return nil
	}
	srcHost := hostKeyOf(base, g, pol)
	out := make([]meguri.Discovery, 0, len(res.Entries))
	for _, e := range res.Entries {
		key, canon, _, ok := dedup.CanonicalKey(e.Loc, base, g, pol)
		if !ok {
			continue
		}
		observed := now
		if e.HasLastMod {
			observed = e.LastMod
		}
		out = append(out, meguri.Discovery{
			URLKey:          key,
			CanonicalURL:    canon,
			Depth:           0, // a sitemap lists a host's URLs at the top, not down a link chain
			DiscoverySource: meguri.SourceSitemap,
			SrcHostKey:      srcHost,
			ObservedAt:      observed,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ChildURLs canonicalizes a sitemapindex's child sitemap locations against base,
// the URLs ami fetches next to walk an index down to its urlsets. It drops a child
// whose location is not a fetchable URL. It carries no discovery: a child is a
// sitemap to fetch, not a page to crawl.
func (res *Result) ChildURLs(base string, pol *dedup.CanonPolicy) []string {
	if len(res.Children) == 0 {
		return nil
	}
	out := make([]string, 0, len(res.Children))
	for _, loc := range res.Children {
		if canon, ok := dedup.Canonicalize(loc, base, pol); ok {
			out = append(out, canon)
		}
	}
	return out
}

// hostKeyOf derives the HostKey of the sitemap's own host, the SrcHostKey every
// discovery from this file carries. A base that does not canonicalize leaves the
// source host zero, which is harmless: the discovery still routes on its own
// URLKey, the source host is only the reputation hint.
func hostKeyOf(base string, g meguri.HostGrouping, pol *dedup.CanonPolicy) uint64 {
	if key, _, _, ok := dedup.CanonicalKey(base, "", g, pol); ok {
		return key.HostKey
	}
	return 0
}
