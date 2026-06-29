package dedup

import (
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/tamnd/meguri"
	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

// CanonPolicy is the per-host canonicalization configuration (doc 03). It is
// versioned and pinned for a campaign: a change to it changes identity for the
// host, so it is a campaign-boundary change, never a mid-flight mutation. A nil
// policy means the global defaults: the tracking deny-list, no folding, case
// preserved.
type CanonPolicy struct {
	QueryAllow    []string // if non-empty, only these query keys survive (allowlist mode)
	QueryDeny     []string // extra deny-list keys beyond the global defaults
	FoldTrailing  bool     // collapse /p and /p/ to /p
	FoldIndex     bool     // collapse /dir/ and /dir/index.html
	FoldCase      bool     // lowercase the path and query (case-insensitive host)
	CollapseSlash bool     // collapse duplicate path slashes
	Version       uint16   // policy version, recorded with the partition
}

// trackingParams is the global deny-list: query keys that never select a
// different resource and only fragment identity (doc 03, step 10). It is the
// safe default for a host with no allowlist.
var trackingParams = map[string]bool{
	"utm_source":   true,
	"utm_medium":   true,
	"utm_campaign": true,
	"utm_term":     true,
	"utm_content":  true,
	"gclid":        true,
	"fbclid":       true,
	"msclkid":      true,
	"mc_eid":       true,
	"ref":          true,
	"ref_src":      true,
	"ref_url":      true,
	"igshid":       true,
	"yclid":        true,
	"_ga":          true,
}

// indexFiles are the directory-index filenames the index-file rule folds when a
// host's policy opts in (doc 03, step 9).
var indexFiles = []string{"index.html", "index.htm", "index.php", "default.asp", "default.aspx"}

// Canonicalize runs the eleven-step canonicalization of doc 03 over a raw link
// found on the page at base (base may be empty when raw is already absolute). It
// returns the canonical URL string and ok=true, or ok=false when the link is not
// a frontier URL (a non-http(s) scheme, an unparseable reference, or an invalid
// host). The function is pure: same input, same output, no clock, no network.
//
// The asymmetry of doc 03 is the rule throughout: a missed merge wastes a little
// space by carrying two records for one page, a wrong merge silently drops a real
// page, so every borderline rule errs toward keeping URLs distinct. Trailing
// slash, path case, index files, duplicate slashes, and encoded delimiters are
// all kept by default and folded only when the host's policy says so.
func Canonicalize(raw, base string, pol *CanonPolicy) (string, bool) {
	// 1. Resolve relative to absolute against the page URL.
	ref, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	if base != "" {
		b, err := url.Parse(base)
		if err == nil {
			ref = b.ResolveReference(ref)
		}
	}
	if !ref.IsAbs() {
		return "", false
	}
	// Resolving an absolute URL against itself runs the RFC 3986 remove-dot-
	// segments algorithm on its path, so /a/b/../c becomes /a/c (step 7).
	ref = ref.ResolveReference(ref)

	// 2. Lowercase the scheme; only http and https survive.
	scheme := strings.ToLower(ref.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false
	}

	// 3. Lowercase and IDN-encode the host, strip the trailing root dot.
	host := strings.ToLower(ref.Hostname())
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return "", false
	}
	if asAscii, err := idna.Lookup.ToASCII(host); err == nil {
		host = asAscii
	} else {
		// A host that fails IDNA validation makes the whole URL invalid.
		return "", false
	}

	// 4. Strip the default port; keep a non-default one as part of identity.
	port := ref.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}

	// 5. Drop the fragment (handled by reading ref.Path/RawQuery only below).

	// 6 + 7. Normalize the path: dot segments are already resolved by
	// ResolveReference and url.Parse; re-encode percent-escapes canonically and
	// set an empty path to "/".
	path := ref.EscapedPath()
	if path == "" {
		path = "/"
	}
	path = normalizePercent(path)

	// 8 + 9. Trailing-slash and index-file folding are per-host opt-ins.
	if pol != nil {
		if pol.CollapseSlash {
			path = collapseSlashes(path)
		}
		if pol.FoldIndex {
			path = foldIndex(path)
		}
		if pol.FoldTrailing && len(path) > 1 {
			path = strings.TrimSuffix(path, "/")
			if path == "" {
				path = "/"
			}
		}
	}

	// 10. Filter and sort the query.
	query := filterQuery(ref.Query(), pol)

	// 11. Path/query case: preserved by default, folded only on opt-in.
	if pol != nil && pol.FoldCase {
		path = strings.ToLower(path)
		query = strings.ToLower(query)
	}

	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	b.WriteString(host)
	if port != "" {
		b.WriteByte(':')
		b.WriteString(port)
	}
	b.WriteString(path)
	if query != "" {
		b.WriteByte('?')
		b.WriteString(query)
	}
	return b.String(), true
}

// filterQuery applies the deny-list (default) or allowlist (policy), then sorts
// the survivors by key and value so parameter order never creates a distinct
// identity. An empty result drops the "?" by returning "".
func filterQuery(values url.Values, pol *CanonPolicy) string {
	if len(values) == 0 {
		return ""
	}
	allow := map[string]bool{}
	if pol != nil {
		for _, k := range pol.QueryAllow {
			allow[k] = true
		}
	}
	deny := map[string]bool{}
	if pol != nil {
		for _, k := range pol.QueryDeny {
			deny[k] = true
		}
	}

	type kv struct{ k, v string }
	var kept []kv
	for k, vs := range values {
		if len(allow) > 0 {
			if !allow[k] {
				continue
			}
		} else if trackingParams[k] || deny[k] {
			continue
		}
		for _, v := range vs {
			kept = append(kept, kv{k, v})
		}
	}
	if len(kept) == 0 {
		return ""
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].k != kept[j].k {
			return kept[i].k < kept[j].k
		}
		return kept[i].v < kept[j].v
	})
	var b strings.Builder
	for i, p := range kept {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(p.k))
		if p.v != "" {
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(p.v))
		}
	}
	return b.String()
}

// normalizePercent decodes the unreserved set (A-Z a-z 0-9 - . _ ~) and
// uppercases every surviving escape to canonical %XX form, never decoding a
// reserved delimiter (doc 03, step 6). A %2F stays %2F so a segment-internal
// slash is not merged with a path separator.
func normalizePercent(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' && i+2 < len(s) {
			hi, ok1 := hexVal(s[i+1])
			lo, ok2 := hexVal(s[i+2])
			if ok1 && ok2 {
				dec := hi<<4 | lo
				if isUnreserved(dec) {
					b.WriteByte(dec)
				} else {
					b.WriteByte('%')
					b.WriteByte(upHex(s[i+1]))
					b.WriteByte(upHex(s[i+2]))
				}
				i += 2
				continue
			}
		}
		b.WriteByte(c)
	}
	return b.String()
}

func isUnreserved(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '.', c == '_', c == '~':
		return true
	}
	return false
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func upHex(c byte) byte {
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 'A'
	}
	return c
}

func collapseSlashes(path string) string {
	var b strings.Builder
	var prevSlash bool
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			if prevSlash {
				continue
			}
			prevSlash = true
		} else {
			prevSlash = false
		}
		b.WriteByte(path[i])
	}
	return b.String()
}

func foldIndex(path string) string {
	slash := strings.LastIndexByte(path, '/')
	if slash < 0 {
		return path
	}
	if slices.Contains(indexFiles, path[slash+1:]) {
		return path[:slash+1]
	}
	return path
}

// RegistrableDomain returns the registrable domain (public-suffix-plus-one) of a
// host, the default host group key (doc 03). It is computed by the embedded
// Public Suffix List: the longest matching public suffix plus the one label to
// its left. A host that is itself a public suffix, or has no label above the
// suffix, returns the host unchanged.
func RegistrableDomain(host string) string {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "" {
		return ""
	}
	d, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return host
	}
	return d
}

// HostGroupKey returns the bytes the HostKey hashes for a canonical host, per
// the grouping mode (doc 03's hostGroupKey).
func HostGroupKey(host string, g meguri.HostGrouping) string {
	switch g {
	case meguri.GroupFullHost:
		return host
	default:
		return RegistrableDomain(host)
	}
}

// Key derives the 128-bit URLKey of a canonical URL under a grouping mode, and
// returns the host group key alongside it. The HostKey hashes the group key, the
// PathKey hashes the canonical-URL remainder "scheme://full_host[:port]path[?query]"
// (doc 03's PathKey byte range). ok is false only when the canonical URL has no
// host.
func Key(canonURL string, g meguri.HostGrouping) (meguri.URLKey, string, bool) {
	u, err := url.Parse(canonURL)
	if err != nil || u.Hostname() == "" {
		return meguri.URLKey{}, "", false
	}
	group := HostGroupKey(u.Hostname(), g)
	return meguri.URLKey{
		HostKey: meguri.HostKeyOf(group),
		PathKey: meguri.PathKeyOf(canonURL),
	}, group, true
}

// CanonicalKey is the discovery-path convenience: canonicalize a raw link and
// derive its URLKey in one call. ok is false when the link is not a frontier URL.
func CanonicalKey(raw, base string, g meguri.HostGrouping, pol *CanonPolicy) (meguri.URLKey, string, string, bool) {
	canon, ok := Canonicalize(raw, base, pol)
	if !ok {
		return meguri.URLKey{}, "", "", false
	}
	key, group, ok := Key(canon, g)
	if !ok {
		return meguri.URLKey{}, "", "", false
	}
	return key, canon, group, true
}
