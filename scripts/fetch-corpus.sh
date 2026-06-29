#!/usr/bin/env bash
# Pull a frozen Common Crawl slice into a local corpus that every milestone gate
# reuses, so meguri's tests and benchmarks run on real URLs, real hosts, real
# HTTP status, and real capture timestamps rather than synthetic data.
#
# The slice is a set of seed domains queried out of one pinned Common Crawl
# index through ccrawl-cli (https://github.com/tamnd/ccrawl-cli). Pinning the
# crawl id makes the corpus reproducible: the same id and the same domains
# always yield the same captures, so a bytes-per-url number measured today is
# comparable to one measured next month.
#
# Usage:
#   scripts/fetch-corpus.sh                 # default domains, default crawl
#   CRAWL=CC-MAIN-2024-10 scripts/fetch-corpus.sh
#   DOMAINS="example.com golang.org" scripts/fetch-corpus.sh
#
# Output:
#   corpus/urls.jsonl   one CDX capture record per line (url, host, status, ...)
#   corpus/MANIFEST      the crawl id, the domains, and the line count
#
# Point the benchmarks at it with:
#   MEGURI_CORPUS=corpus/urls.jsonl go test -run Corpus -bench Corpus ./format
set -euo pipefail

CRAWL="${CRAWL:-CC-MAIN-2024-10}"
OUT_DIR="${OUT_DIR:-corpus}"
DOMAINS="${DOMAINS:-en.wikipedia.org github.com golang.org rust-lang.org python.org nytimes.com bbc.co.uk stackoverflow.com mozilla.org apache.org}"

if ! command -v ccrawl >/dev/null 2>&1; then
  echo "ccrawl not found on PATH. Install it from https://github.com/tamnd/ccrawl-cli" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
URLS="$OUT_DIR/urls.jsonl"
: >"$URLS"

echo "Pulling captures from $CRAWL for: $DOMAINS"
for d in $DOMAINS; do
  echo "  $d"
  # Every captured URL under the domain, as CDX JSONL: url, host, status,
  # timestamp, mime, digest, length. Failures on one domain do not abort the
  # whole pull, so a single dead seed does not cost the corpus.
  ccrawl search "$d/*" --crawl "$CRAWL" -o jsonl >>"$URLS" 2>/dev/null || \
    echo "    (no captures or query failed for $d, skipping)" >&2
done

LINES=$(wc -l <"$URLS" | tr -d ' ')
{
  echo "crawl=$CRAWL"
  echo "domains=$DOMAINS"
  echo "records=$LINES"
} >"$OUT_DIR/MANIFEST"

echo "Wrote $LINES records to $URLS"
echo "Run the corpus benchmark with:"
echo "  MEGURI_CORPUS=$URLS go test -run Corpus -bench Corpus ./format"
echo
echo "The benchmark slice is also pinned as a host-key range (bench/corpus.go), the"
echo "interval a fleet partition carries in its .meguri header rather than a domain"
echo "list. Print it and check it against the pin with:"
echo "  MEGURI_CORPUS=$URLS go test -run CorpusHostKeyRangePin ./bench"
echo "  meguri bench -i $URLS   # prints the host-key range line"
