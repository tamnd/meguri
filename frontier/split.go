package frontier

import "strings"

// HostOf and PathOf split a URL into the host grouping and the path-and-query
// remainder the URLKey halves hash from. They are the deliberately minimal
// split the discovery path uses until full canonicalization lands with the
// seen-set in M2: no scheme normalization, no PSL folding, no default-port or
// case handling. They match the fallback the format's corpus loader uses, so a
// URL keys the same whether it enters through the loader or through Seed.
func HostOf(u string) string {
	s := stripScheme(u)
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		return s[:i]
	}
	return s
}

// PathOf returns the path-and-query remainder, defaulting to "/" when the URL
// is bare host only.
func PathOf(u string) string {
	s := stripScheme(u)
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		return s[i:]
	}
	return "/"
}

func stripScheme(u string) string {
	if _, after, found := strings.Cut(u, "://"); found {
		return after
	}
	return u
}
