#!/usr/bin/env bash
# Pull a realistic, broad multi-host corpus from Common Crawl via ccrawl-cli.
#
# The columnar index is SURT-sorted, so a single-TLD scan clusters by host (a .com
# head is all one parking host). Single-page-heavy TLDs (.dev, .app, .page, .xyz)
# scatter across thousands of distinct hosts instead, and mixing several of them
# with a heavy real domain gives a Zipfian shape (a few big hosts, a long tail of
# single-page hosts) that lands across all 256 DRUM buckets. That is the realistic
# frontier shape the scale harness needs, not a synthetic one.
#
# Output is url-only JSONL; the seed and scale paths derive the host from the URL,
# so we keep the corpus slim (no full CDX) to fit a tight disk budget.
set -euo pipefail

OUT="${1:-corpus/build/wide.jsonl}"
CRAWL="${CRAWL:-latest}"
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

# tld:count pairs. The scatter TLDs carry the host tail; the counts are sized to
# reach ~1.1M rows before dedup, which collapses to ~1M distinct URLs.
PULLS=(
  "dev:300000"
  "app:200000"
  "xyz:200000"
  "page:120000"
  "io:120000"
  "me:120000"
)

for pull in "${PULLS[@]}"; do
  tld="${pull%%:*}"
  n="${pull##*:}"
  echo "[$(date +%H:%M:%S)] pulling $n from .$tld ..."
  ccrawl table urls --tld "$tld" --status 200 -n "$n" -c "$CRAWL" -o jsonl 2>/dev/null \
    | python3 -c "import sys,json
for line in sys.stdin:
    try: u=json.loads(line).get('url','')
    except Exception: continue
    if u: sys.stdout.write(json.dumps({'url':u})+'\n')" >> "$OUT" || echo "  (.$tld pull failed, continuing)"
  echo "  total rows so far: $(wc -l < "$OUT")"
done

echo "[$(date +%H:%M:%S)] raw pull done: $(wc -l < "$OUT") rows -> $OUT"
