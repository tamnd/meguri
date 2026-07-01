// Package dataset converts a meguri live store to and from Apache Parquet, so a
// frontier snapshot can be published as a Hugging Face dataset and read back into
// a .meguri file. The .meguri format is meguri's own columnar store, tuned for the
// crawl loop (a resident seen-set filter, a due-scan cursor, host clustering);
// Parquet is the interchange format the data ecosystem reads (the Hugging Face
// dataset viewer, datasets, pyarrow, duckdb, polars). This package is the bridge:
// export streams the URL table out to Parquet in key order, import streams Parquet
// back into a fresh .meguri, and the pair round-trips every field.
//
// The unit of publication is the URL table: one row per frontier entry, the crawl
// state plus the resolved URL and host strings. The host table (DNS, robots,
// politeness) is operational state that does not belong in a public dataset, so it
// is not exported; a re-import rebuilds a minimal host table from the URL rows'
// host strings, which is all a fresh frontier needs.
package dataset

import (
	"time"

	m "github.com/tamnd/meguri"
)

// SchemaVersion is stamped into the manifest and the dataset card. It bumps when the
// Row layout changes so a reader can tell an old dump from a new one.
const SchemaVersion = 1

// epochHoursUnix is the meguri epoch: field times are stored as uint32 hours since
// the Unix epoch (doc 10), so an hour value multiplied by this is Unix seconds. The
// zero hour means "never" (never crawled, no Last-Modified), which maps to a nil
// timestamp in Parquet rather than the year-1970 boundary, so the viewer shows an
// empty cell instead of a misleading 1970 date.
const secondsPerHour = 3600

// Row is the Parquet shape of one URL-table entry. Every URLRecord field is carried
// so the round-trip is lossless, with three readability additions the raw record
// leaves implicit: the resolved url, host, and etag strings (the record holds only
// arena offsets), and human-readable status and source names alongside the numeric
// codes. The epoch-hour timestamps become real Parquet TIMESTAMP columns so the
// Hugging Face viewer renders dates, not integers; never-set times are nullable and
// come back nil.
//
// The field order is the column order in the file. Keys and strings lead (what a
// human scans first), then the crawl-state numerics, then the fingerprints and
// error counters. The `zstd` tag on the big string columns compresses them per
// column; the low-cardinality name and code columns dictionary-encode to near
// nothing on their own.
type Row struct {
	// Identity: the 128-bit URLKey split into its two halves, and the strings.
	HostKey uint64 `parquet:"host_key"`
	PathKey uint64 `parquet:"path_key"`
	URL     string `parquet:"url,zstd"`
	Host    string `parquet:"host,zstd,dict"`

	// State machine and discovery.
	Status     string  `parquet:"status,dict"`
	StatusCode uint8   `parquet:"status_code"`
	Priority   float32 `parquet:"priority"`
	Depth      uint16  `parquet:"depth"`
	Source     string  `parquet:"source,dict"`
	SourceCode uint8   `parquet:"source_code"`

	// Lifecycle timestamps, all nullable. A freshly seeded frontier carries none of
	// them (a URL is discovered but not yet dated or scheduled), so every one is nil
	// until its event happens: the zero hour is a null column, not a boundary date.
	// This also keeps the columns inside the representable timestamp range; a non-null
	// value is a real epoch-hour a scheduler set.
	FirstSeen    *time.Time `parquet:"first_seen,timestamp(millisecond),optional"`
	NextDue      *time.Time `parquet:"next_due,timestamp(millisecond),optional"`
	LastCrawled  *time.Time `parquet:"last_crawled,timestamp(millisecond),optional"`
	LastChanged  *time.Time `parquet:"last_changed,timestamp(millisecond),optional"`
	LastModified *time.Time `parquet:"last_modified,timestamp(millisecond),optional"`

	// Change model and counters.
	Lambda         float32 `parquet:"lambda"`
	CrawlCount     uint32  `parquet:"crawl_count"`
	ChangeCount    uint32  `parquet:"change_count"`
	NoChangeStreak uint16  `parquet:"no_change_streak"`

	// Validators and content fingerprints.
	ETag      string `parquet:"etag,zstd,optional"`
	ContentFP uint64 `parquet:"content_fp"`
	Simhash   uint64 `parquet:"simhash"`

	// Last-crawl outcome.
	HTTPStatus  uint16 `parquet:"http_status"`
	RedirectRef uint64 `parquet:"redirect_ref"`
	RetryCount  uint8  `parquet:"retry_count"`
	ErrorCount  uint16 `parquet:"error_count"`
}

// hoursToTime converts a meguri epoch-hour value to a UTC time. The zero hour is the
// "never" sentinel and maps to the zero time, which callers building nullable columns
// treat as nil.
func hoursToTime(h uint32) time.Time {
	if h == 0 {
		return time.Time{}
	}
	return time.Unix(int64(h)*secondsPerHour, 0).UTC()
}

// hoursToPtr is the nullable form: the zero hour becomes a nil pointer so the Parquet
// column is null, not a year-1970 date.
func hoursToPtr(h uint32) *time.Time {
	if h == 0 {
		return nil
	}
	t := hoursToTime(h)
	return &t
}

// timeToHours reverses hoursToTime, rounding to the nearest hour so a value that
// survived a millisecond-timestamp round-trip lands back on its exact epoch-hour. A
// zero time (a null column) is the "never" sentinel and returns 0.
func timeToHours(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	return uint32(t.Unix() / secondsPerHour)
}

// ptrToHours is the nullable form of timeToHours: a nil pointer is "never" and returns 0.
func ptrToHours(t *time.Time) uint32 {
	if t == nil {
		return 0
	}
	return timeToHours(*t)
}

// ToRow builds the Parquet row for a record and its resolved strings. The record
// carries arena offsets (URLRef, ETagRef) that mean nothing outside the file it came
// from, so the caller resolves them to strings and passes them in; RedirectRef is an
// internal record reference kept raw for fidelity but not portable across a rebuild.
func ToRow(r *m.URLRecord, url, host, etag string) Row {
	return Row{
		HostKey:        r.URLKey.HostKey,
		PathKey:        r.URLKey.PathKey,
		URL:            url,
		Host:           host,
		Status:         r.Status.String(),
		StatusCode:     uint8(r.Status),
		Priority:       r.Priority,
		Depth:          r.Depth,
		Source:         sourceName(r.DiscoverySource),
		SourceCode:     uint8(r.DiscoverySource),
		FirstSeen:      hoursToPtr(r.FirstSeen),
		NextDue:        hoursToPtr(r.NextDue),
		LastCrawled:    hoursToPtr(r.LastCrawled),
		LastChanged:    hoursToPtr(r.LastChanged),
		LastModified:   hoursToPtr(r.LastModified),
		Lambda:         r.Lambda,
		CrawlCount:     r.CrawlCount,
		ChangeCount:    r.ChangeCount,
		NoChangeStreak: r.NoChangeStreak,
		ETag:           etag,
		ContentFP:      r.ContentFP,
		Simhash:        r.Simhash,
		HTTPStatus:     r.HTTPStatus,
		RedirectRef:    r.RedirectRef,
		RetryCount:     r.RetryCount,
		ErrorCount:     r.ErrorCount,
	}
}

// FromRow reconstructs a record from a Parquet row, and returns the url, host, and
// etag strings the caller must re-intern into the new file's arena (the URLRef and
// ETagRef offsets are assigned by the build, not carried over). The numeric status
// and source codes are the source of truth; the name columns are for readers and are
// ignored here, so a row with an unknown name still decodes to the right code.
func FromRow(row *Row) (rec m.URLRecord, url, host, etag string) {
	rec = m.URLRecord{
		URLKey:          m.URLKey{HostKey: row.HostKey, PathKey: row.PathKey},
		HostKey:         row.HostKey,
		Status:          m.URLStatus(row.StatusCode),
		Priority:        row.Priority,
		Depth:           row.Depth,
		DiscoverySource: m.DiscoverySource(row.SourceCode),
		FirstSeen:       ptrToHours(row.FirstSeen),
		NextDue:         ptrToHours(row.NextDue),
		LastCrawled:     ptrToHours(row.LastCrawled),
		LastChanged:     ptrToHours(row.LastChanged),
		LastModified:    ptrToHours(row.LastModified),
		Lambda:          row.Lambda,
		CrawlCount:      row.CrawlCount,
		ChangeCount:     row.ChangeCount,
		NoChangeStreak:  row.NoChangeStreak,
		ContentFP:       row.ContentFP,
		Simhash:         row.Simhash,
		HTTPStatus:      row.HTTPStatus,
		RedirectRef:     row.RedirectRef,
		RetryCount:      row.RetryCount,
		ErrorCount:      row.ErrorCount,
	}
	return rec, row.URL, row.Host, row.ETag
}

// sourceName is the human-readable discovery source, the source-column analogue of
// URLStatus.String. An out-of-range value prints its number so a forward-compatible
// reader never loses information.
func sourceName(s m.DiscoverySource) string {
	switch s {
	case m.SourceSeed:
		return "seed"
	case m.SourceLink:
		return "link"
	case m.SourceSitemap:
		return "sitemap"
	case m.SourceRedirect:
		return "redirect"
	case m.SourceManual:
		return "manual"
	default:
		return "source(" + itoa(int(s)) + ")"
	}
}

// itoa is a tiny local integer format so the schema file carries no import beyond
// time and the meguri record types.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
