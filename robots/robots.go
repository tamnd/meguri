// Package robots parses robots.txt per RFC 9309 and answers the one question
// the frontier asks: may meguri fetch this path?
//
// The parser reduces a whole file to a single Rules value already specialized
// to meguri's user agent. It picks the most specific matching group, keeps that
// group's Allow and Disallow rules and Crawl-delay, and gathers the file-global
// Sitemap lines. Everything else is dropped.
//
// The matcher follows the Google lineage that RFC 9309 standardized: longest
// pattern match wins, an Allow breaks a tie against an equally long Disallow,
// and only the two metacharacters * and $ are special.
package robots

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
	"time"
)

// MaxBytes is the RFC 9309 parse cap: a crawler must parse at least the first
// 500 KiB of a robots.txt and may stop after that.
// We stop exactly there so a hostile or runaway file cannot cost us more.
const MaxBytes = 500 << 10

// Rules is meguri's compact parsed form of one robots.txt, already reduced to
// the single group that applies to meguri's user agent.
// A nil *Rules means allow-all: no file, an empty file, or no matching group.
type Rules struct {
	// rules is the matched group's Allow and Disallow patterns, kept in the
	// order they appeared so a tie at equal length is stable.
	rules []rule
	// crawlDelay is the group's Crawl-delay, zero when none was published.
	crawlDelay time.Duration
	// sitemaps is the file-global list of Sitemap URLs.
	sitemaps []string
}

// rule is one Allow or Disallow line from the matched group.
type rule struct {
	pattern string
	allow   bool
	// length is the match weight: the literal character count of the pattern,
	// with a trailing $ anchor not counted. This is what longest-match compares.
	length int
}

// Parse parses a robots.txt body and returns the rules for userAgent, matched
// case-insensitively as a prefix and falling back to the * group.
// It strips a leading UTF-8 BOM, tolerates malformed lines, and reads at most
// MaxBytes.
// A nil result means allow-all.
func Parse(body []byte, userAgent string) *Rules {
	if len(body) > MaxBytes {
		body = body[:MaxBytes]
	}
	body = bytes.TrimPrefix(body, []byte{0xEF, 0xBB, 0xBF})

	// product is the token we prefix-match group user-agents against. A real
	// agent string like "meguri/1.0" matches a group declared as "meguri".
	product := strings.ToLower(userAgent)

	// groups collects every group in file order. A group is a run of one or
	// more User-agent lines followed by its rules. Crawl-delay rides along with
	// the group it sits in.
	var groups []group
	var cur *group
	// startingGroup is true while we are still reading the User-agent lines that
	// open a group. A rule line ends that run, so the next User-agent line after
	// a rule begins a fresh group.
	startingGroup := false

	var sitemaps []string

	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64<<10), MaxBytes)
	for sc.Scan() {
		field, value, ok := splitLine(sc.Text())
		if !ok {
			continue
		}
		switch field {
		case "user-agent", "useragent":
			if !startingGroup {
				groups = append(groups, group{})
				cur = &groups[len(groups)-1]
				startingGroup = true
			}
			cur.agents = append(cur.agents, strings.ToLower(value))
		case "allow", "disallow":
			if cur == nil {
				// A rule before any User-agent line has no group to attach to.
				continue
			}
			startingGroup = false
			cur.rules = append(cur.rules, rule{
				pattern: value,
				allow:   field == "allow",
				length:  patternLength(value),
			})
		case "crawl-delay":
			if cur == nil {
				continue
			}
			startingGroup = false
			if d, ok := parseDelay(value); ok {
				cur.crawlDelay = d
			}
		case "sitemap":
			if value != "" {
				sitemaps = append(sitemaps, value)
			}
		default:
			// Unknown directive. Skip it without disturbing the current group.
		}
	}

	best := selectGroup(groups, product)
	if best == nil && len(sitemaps) == 0 {
		return nil
	}
	r := &Rules{sitemaps: sitemaps}
	if best != nil {
		r.rules = best.rules
		r.crawlDelay = best.crawlDelay
	}
	return r
}

// group is one robots.txt group while we are still accumulating the file.
type group struct {
	agents     []string
	rules      []rule
	crawlDelay time.Duration
}

// splitLine strips a comment, trims space, and splits a directive line into a
// lowercased field name and its raw value. ok is false for blank or fieldless
// lines, which the caller skips.
func splitLine(line string) (field, value string, ok bool) {
	if i := strings.IndexByte(line, '#'); i >= 0 {
		line = line[:i]
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", "", false
	}
	colon := strings.IndexByte(line, ':')
	if colon < 0 {
		// No colon means we cannot tell field from value. Tolerate and skip.
		return "", "", false
	}
	field = strings.ToLower(strings.TrimSpace(line[:colon]))
	value = strings.TrimSpace(line[colon+1:])
	if field == "" {
		return "", "", false
	}
	return field, value, true
}

// selectGroup picks the most specific group whose user-agent token is a prefix
// of product. The longest matching token wins, the * group is the fallback, and
// a nil result means no group applies at all.
func selectGroup(groups []group, product string) *group {
	var best *group
	bestLen := -1
	var wildcard *group
	for i := range groups {
		g := &groups[i]
		for _, agent := range g.agents {
			if agent == "*" {
				if wildcard == nil {
					wildcard = g
				}
				continue
			}
			if strings.HasPrefix(product, agent) && len(agent) > bestLen {
				best = g
				bestLen = len(agent)
			}
		}
	}
	if best != nil {
		return best
	}
	return wildcard
}

// patternLength is the longest-match weight of a pattern: the count of literal
// characters, with a trailing $ anchor not counted. A * counts as the one
// character it is written as, which matches the Google-lineage worked examples.
func patternLength(p string) int {
	if strings.HasSuffix(p, "$") {
		return len(p) - 1
	}
	return len(p)
}

// parseDelay reads a Crawl-delay value as seconds, which may be fractional, and
// returns it as a Duration. ok is false when the value is not a number.
func parseDelay(value string) (time.Duration, bool) {
	secs, err := strconv.ParseFloat(value, 64)
	if err != nil || secs < 0 {
		return 0, false
	}
	return time.Duration(secs * float64(time.Second)), true
}

// Allowed reports whether path may be fetched. path is a percent-decoded URL
// path such as "/a/b?x=1".
// A nil receiver allows everything.
// The decision is the longest-match rule, with an Allow winning a tie against
// an equally long Disallow.
func (r *Rules) Allowed(path string) bool {
	if r == nil || len(r.rules) == 0 {
		return true
	}
	// bestLen is the weight of the strongest matching rule so far, allow its
	// verdict. We start below zero so any real match takes over.
	bestLen := -1
	allow := true
	for _, rl := range r.rules {
		if !match(rl.pattern, path) {
			continue
		}
		switch {
		case rl.length > bestLen:
			bestLen = rl.length
			allow = rl.allow
		case rl.length == bestLen && rl.allow:
			// Equal longest match: Allow breaks the tie.
			allow = true
		}
	}
	return allow
}

// CrawlDelay returns the group's Crawl-delay, or 0 when none was published.
func (r *Rules) CrawlDelay() time.Duration {
	if r == nil {
		return 0
	}
	return r.crawlDelay
}

// Sitemaps returns the absolute sitemap URLs named by Sitemap: lines.
// These are file-global in robots.txt, not per-group.
func (r *Rules) Sitemaps() []string {
	if r == nil {
		return nil
	}
	return r.sitemaps
}

// match reports whether a robots.txt pattern matches path. Only two
// metacharacters are special: * matches any run of characters, and a trailing $
// anchors the pattern to the end of path. Everything else is literal.
// An empty pattern matches nothing, which is how an empty Disallow disallows
// nothing.
func match(pattern, path string) bool {
	if pattern == "" {
		return false
	}
	anchored := false
	if strings.HasSuffix(pattern, "$") {
		anchored = true
		pattern = pattern[:len(pattern)-1]
	}
	// segments are the literal runs between * wildcards. The first segment must
	// sit at the start of path, each later segment must appear in order after
	// the previous one, and under $ the last segment must land at the very end.
	segments := strings.Split(pattern, "*")

	pos := 0
	for i, seg := range segments {
		if seg == "" {
			continue
		}
		if i == 0 {
			if !strings.HasPrefix(path[pos:], seg) {
				return false
			}
			pos += len(seg)
			continue
		}
		if i == len(segments)-1 && anchored {
			if !strings.HasSuffix(path[pos:], seg) {
				return false
			}
			pos = len(path)
			continue
		}
		idx := strings.Index(path[pos:], seg)
		if idx < 0 {
			return false
		}
		pos += idx + len(seg)
	}
	// Under $ with a trailing wildcard (pattern ended in *$, an empty last
	// segment) or a plain anchor, the last literal must reach the end of path.
	if anchored && lastSegmentEmpty(segments) {
		return true
	}
	if anchored && pos != len(path) {
		return false
	}
	return true
}

// lastSegmentEmpty reports whether the pattern ended in a wildcard, so the final
// split segment is empty and the $ anchor is satisfied by any remaining tail.
func lastSegmentEmpty(segments []string) bool {
	return len(segments) > 0 && segments[len(segments)-1] == ""
}
