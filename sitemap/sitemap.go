// Package sitemap parses the Sitemaps 0.9 protocol (sitemaps.org) into the
// discoveries the frontier feeds on. It reads a <urlset> as a list of page URLs
// and a <sitemapindex> as a list of child sitemap files to fetch next.
//
// The parser is deliberately narrow. It keeps <loc> and <lastmod> and drops
// changefreq and priority: those two are self-declared, unverifiable, and almost
// always wrong, so meguri does not let them steer the crawl. lastmod survives
// only as a weak freshness prior, parsed leniently so one bad date never sinks a
// whole file.
//
// The reader is hardened for the open web. It detects gzip by magic bytes and
// decompresses transparently, caps the decompressed body at MaxDecompressed to
// blunt a gzip bomb, streams the XML so a 50 MB file never materializes at once,
// and stops after MaxEntries.
package sitemap

import (
	"bufio"
	"compress/gzip"
	"encoding/xml"
	"io"
	"strings"
	"time"
)

// Entry is one <url> from a urlset. meguri consumes Loc as a discovery and
// LastMod as a weak freshness prior. changefreq and priority are deliberately
// NOT parsed: they are self-declared, unverifiable, and almost always wrong.
type Entry struct {
	Loc        string // the <loc> URL, the only required field
	LastMod    uint32 // <lastmod> as epoch-hours, 0 when absent or unparseable
	HasLastMod bool   // true when a parseable <lastmod> was present
}

// Result is one parsed sitemap. Exactly one of Entries or Children is populated
// in practice: a <urlset> yields Entries, a <sitemapindex> yields Children
// (the URLs of child sitemap files to fetch next).
type Result struct {
	Entries  []Entry  // from <urlset><url><loc>
	Children []string // from <sitemapindex><sitemap><loc>
}

// MaxDecompressed caps the decompressed body to defend against a gzip bomb.
const MaxDecompressed = 50 << 20 // 50 MB, the protocol's uncompressed limit

// MaxEntries caps the number of URLs parsed from one sitemap file.
const MaxEntries = 50_000

// lastModLayouts are the W3C datetime forms a <lastmod> may take, in the order
// the protocol allows. We accept date-only, the two zoned datetime spellings,
// and full RFC3339. A value that matches none of these leaves LastMod=0 and
// HasLastMod=false rather than failing the file.
var lastModLayouts = []string{
	"2006-01-02",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05-07:00",
	time.RFC3339,
}

// Parse reads a sitemap from r. It transparently decompresses gzip, caps the
// decompressed bytes at MaxDecompressed and the entries at MaxEntries, ignores
// changefreq and priority, and parses <lastmod> in W3C datetime form. It
// returns a Result with either Entries (urlset) or Children (sitemapindex).
func Parse(r io.Reader) (*Result, error) {
	body, err := decompress(r)
	if err != nil {
		return nil, err
	}
	// Cap the bytes the XML decoder may pull, so a gzip bomb cannot exhaust
	// memory even if it advertises a small header.
	limited := io.LimitReader(body, MaxDecompressed)
	return decode(limited)
}

// decompress peeks the first two bytes for the gzip magic (0x1f 0x8b) and wraps
// the stream in a gzip reader when present. A plain XML body reads straight
// through the buffered reader.
func decompress(r io.Reader) (io.Reader, error) {
	br := bufio.NewReader(r)
	magic, err := br.Peek(2)
	if err != nil {
		if err == io.EOF {
			// A body shorter than two bytes cannot be gzip, so read it raw.
			return br, nil
		}
		return nil, err
	}
	if magic[0] == 0x1f && magic[1] == 0x8b {
		return gzip.NewReader(br)
	}
	return br, nil
}

// decode runs the streaming XML token loop. It learns the document kind from the
// root element name (urlset versus sitemapindex), collects <loc> and <lastmod>
// from each child, and stops once MaxEntries are gathered.
func decode(r io.Reader) (*Result, error) {
	dec := xml.NewDecoder(r)
	res := &Result{}

	// index is set once we see the root: true for a sitemapindex, false for a
	// urlset. We do not key on it before the root arrives.
	var index, sawRoot bool

	// These track the element we are inside so character data lands in the right
	// field. inLoc and inLastMod are true only between an open and close tag.
	var inEntry, inLoc, inLastMod bool
	var loc strings.Builder
	var lastMod strings.Builder

	flush := func() {
		// flush commits the entry we just finished reading. An entry with no
		// <loc> is dropped: it is not a discovery.
		text := strings.TrimSpace(loc.String())
		if text != "" {
			if index {
				res.Children = append(res.Children, text)
			} else {
				e := Entry{Loc: text}
				if h, ok := parseLastMod(strings.TrimSpace(lastMod.String())); ok {
					e.LastMod = h
					e.HasLastMod = true
				}
				res.Entries = append(res.Entries, e)
			}
		}
		loc.Reset()
		lastMod.Reset()
	}

	// We loop while under the cap. Once we have all we are allowed to keep, we
	// stop reading the rest of the file rather than scanning to the end.
	for count(res, index) < MaxEntries {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			// A truncated or malformed body returns what we parsed so far
			// instead of discarding good entries over a bad tail.
			return res, nil
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch local(t.Name) {
			case "urlset":
				if !sawRoot {
					index, sawRoot = false, true
				}
			case "sitemapindex":
				if !sawRoot {
					index, sawRoot = true, true
				}
			case "url", "sitemap":
				inEntry = true
				loc.Reset()
				lastMod.Reset()
			case "loc":
				if inEntry {
					inLoc = true
				}
			case "lastmod":
				if inEntry {
					inLastMod = true
				}
			}
		case xml.CharData:
			if inLoc {
				loc.Write(t)
			} else if inLastMod {
				lastMod.Write(t)
			}
		case xml.EndElement:
			switch local(t.Name) {
			case "loc":
				inLoc = false
			case "lastmod":
				inLastMod = false
			case "url", "sitemap":
				if inEntry {
					flush()
					inEntry = false
				}
			}
		}
	}
	return res, nil
}

// count returns how many discoveries we have collected so far, reading the side
// of Result the document kind populates.
func count(res *Result, index bool) int {
	if index {
		return len(res.Children)
	}
	return len(res.Entries)
}

// local returns the namespace-agnostic element name. Real sitemaps declare the
// 0.9 namespace, so we match on Local and ignore the Space, lowercased so an
// oddly cased document still parses.
func local(name xml.Name) string {
	return strings.ToLower(name.Local)
}

// parseLastMod converts a W3C datetime string into epoch-hours (unix seconds
// divided by 3600, as uint32). It returns ok=false when the value is empty or
// matches no accepted layout, so the caller leaves LastMod=0, HasLastMod=false.
func parseLastMod(s string) (uint32, bool) {
	if s == "" {
		return 0, false
	}
	for _, layout := range lastModLayouts {
		t, err := time.Parse(layout, s)
		if err != nil {
			continue
		}
		secs := t.Unix()
		if secs < 0 {
			// A pre-epoch date cannot be an epoch-hours uint32, so treat it as
			// no date rather than wrapping into a huge value.
			return 0, false
		}
		return uint32(secs / 3600), true
	}
	return 0, false
}
