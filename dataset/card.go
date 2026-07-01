package dataset

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CardName is the fixed filename of the dataset card at the repo root. Hugging Face
// renders it as the dataset landing page and reads its YAML frontmatter to wire the
// dataset viewer to the parquet files.
const CardName = "README.md"

// column documents one Parquet column for the card's data dictionary.
type column struct {
	name, typ, desc string
}

// columns is the data dictionary in file order. It mirrors the Row struct; keeping the
// text here rather than deriving it from struct tags means the card reads like a human
// wrote it, with a real sentence per column.
var columns = []column{
	{"host_key", "uint64", "xxHash64 of the host grouping string, the high half of the 128-bit URL key and the partition key."},
	{"path_key", "uint64", "hash of the path and query, the low half of the URL key."},
	{"url", "string", "the canonical URL."},
	{"host", "string", "the host grouping string the host_key hashes, a registrable domain or a full host."},
	{"status", "string", "the frontier state machine state: discovered, scheduled, ready, in_flight, crawled, due_recrawl, gone, excluded_robots, trapped."},
	{"status_code", "uint8", "the numeric status, the source of truth the name column mirrors."},
	{"priority", "float32", "crawl priority, OPIC cash plus any imported PageRank."},
	{"depth", "uint16", "link distance from the nearest seed."},
	{"source", "string", "how the URL was discovered: seed, link, sitemap, redirect, manual."},
	{"source_code", "uint8", "the numeric discovery source."},
	{"first_seen", "timestamp?", "when the URL was first discovered, null if not yet dated."},
	{"next_due", "timestamp?", "when the URL is next scheduled to be crawled, null if not yet scheduled."},
	{"last_crawled", "timestamp?", "last successful fetch, null if never crawled."},
	{"last_changed", "timestamp?", "last observed content change, null if never seen to change."},
	{"last_modified", "timestamp?", "the Last-Modified the server reported, null if none."},
	{"lambda", "float32", "estimated Poisson change rate in changes per hour, the recrawl model input."},
	{"crawl_count", "uint32", "successful fetches over the URL's lifetime."},
	{"change_count", "uint32", "fetches that observed a content change."},
	{"no_change_streak", "uint16", "consecutive fetches with no change."},
	{"etag", "string?", "the ETag validator, null if none. Empty for a live-store export."},
	{"content_fp", "uint64", "fingerprint of the last body, for exact-duplicate detection."},
	{"simhash", "uint64", "near-duplicate signature of the last body."},
	{"http_status", "uint16", "HTTP status of the last crawl."},
	{"redirect_ref", "uint64", "internal reference to a redirect-target record, kept for fidelity, zero in a live-store export."},
	{"retry_count", "uint8", "consecutive transient failures."},
	{"error_count", "uint16", "failed fetches over the URL's lifetime."},
}

// writeCard writes the dataset card. The YAML frontmatter is the part Hugging Face
// parses: the configs block points the dataset viewer at every data/*.parquet file as
// one train split, so the dataset is browsable the moment it is pushed. The prose below
// is the human landing page.
func writeCard(dir string, man Manifest) error {
	var b strings.Builder

	b.WriteString("---\n")
	b.WriteString("license: odc-by\n")
	b.WriteString("pretty_name: meguri crawl frontier\n")
	b.WriteString("size_categories:\n")
	b.WriteString("- " + sizeCategory(man.Rows) + "\n")
	b.WriteString("tags:\n")
	b.WriteString("- web-crawl\n")
	b.WriteString("- url-frontier\n")
	b.WriteString("- meguri\n")
	b.WriteString("configs:\n")
	b.WriteString("- config_name: default\n")
	b.WriteString("  data_files:\n")
	b.WriteString("  - split: train\n")
	b.WriteString("    path: data/*.parquet\n")
	b.WriteString("---\n\n")

	b.WriteString("# meguri crawl frontier\n\n")
	b.WriteString("This dataset is a snapshot of a [meguri](https://github.com/tamnd/meguri) crawl frontier: one row per URL, carrying the crawl state a scheduler keeps plus the canonical URL and its host. It was exported straight from a `.meguri` live store, and it imports back into one without loss.\n\n")

	fmt.Fprintf(&b, "It holds %s URLs across %s hosts, written as %d Parquet file(s) with %s column compression.\n\n",
		humanInt(man.Rows), humanInt(int64(man.Hosts)), len(man.Files), man.Codec)

	b.WriteString("## Rows are sorted by URL key\n\n")
	b.WriteString("The rows are in `(host_key, path_key)` order, so a host's URLs are contiguous. A reader that wants one site can range-scan it without touching the rest.\n\n")

	b.WriteString("## Columns\n\n")
	b.WriteString("| column | type | description |\n")
	b.WriteString("| --- | --- | --- |\n")
	for _, c := range columns {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c.name, c.typ, c.desc)
	}
	b.WriteString("\nA `?` on a type marks a nullable column. Timestamps are UTC; a null timestamp means the event never happened (never crawled, no Last-Modified).\n\n")

	b.WriteString("## Reading it\n\n")
	b.WriteString("It is plain Parquet, so anything in the ecosystem reads it:\n\n")
	b.WriteString("```python\n")
	b.WriteString("import duckdb\n")
	b.WriteString("duckdb.sql(\"select host, count(*) from 'data/*.parquet' group by 1 order by 2 desc limit 20\")\n")
	b.WriteString("```\n\n")
	b.WriteString("```python\n")
	b.WriteString("from datasets import load_dataset\n")
	b.WriteString("ds = load_dataset(\"<user>/<repo>\", split=\"train\")\n")
	b.WriteString("```\n\n")
	b.WriteString("To load it back into a meguri store: `meguri dataset import --in <repo> --out frontier.meguri`.\n\n")

	b.WriteString("## Incremental dumps\n\n")
	b.WriteString("Each dump adds Parquet files under `data/` and advances the `watermark_hours` in `manifest.json` (the max `last_changed` seen). A later dump can export only the rows that changed past that watermark, and pushing it is a commit on top, so the dataset grows without a full rewrite.\n\n")

	fmt.Fprintf(&b, "Schema version %d.\n", man.SchemaVersion)

	return os.WriteFile(filepath.Join(dir, CardName), []byte(b.String()), 0o644)
}

// sizeCategory maps a row count to a Hugging Face size bucket.
func sizeCategory(rows int64) string {
	switch {
	case rows < 1_000:
		return "n<1K"
	case rows < 10_000:
		return "1K<n<10K"
	case rows < 100_000:
		return "10K<n<100K"
	case rows < 1_000_000:
		return "100K<n<1M"
	case rows < 10_000_000:
		return "1M<n<10M"
	case rows < 100_000_000:
		return "10M<n<100M"
	case rows < 1_000_000_000:
		return "100M<n<1B"
	default:
		return "n>1B"
	}
}

// humanInt renders n with thousands separators for the prose.
func humanInt(n int64) string {
	s := itoa(int(n))
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
