#!/usr/bin/env bash
# Pull a ~100M-row realistic corpus from Common Crawl via ccrawl-cli, the 10x sibling
# of pull-corpus-10m.sh. Same TLD-scatter shape (a long single-page tail across many
# scatter TLDs so no single TLD has to carry a count past its distinct-host volume in
# the index), counts sized to reach ~105M rows before dedup, which collapses to ~100M
# distinct URLs. This is a multi-day job: the CC index fetch runs at roughly 200
# urls/s, so 105M rows is on the order of 6 days; spread the TLDs across several CC
# index segments (CRAWL) and run it under nohup. Output is url-only JSONL, the host
# derived at seed time. The result is REAL crawl URLs, not synthetic, the same
# provenance as scale-10m.
set -euo pipefail

OUT="${1:-corpus/build/wide-100m.jsonl}"
CRAWL="${CRAWL:-latest}"

# Cache: the pinned profile is the durable copy of the real seed. If it is already
# pinned, hand it back and skip the multi-day pull entirely, so a re-run never
# re-downloads the seed (the cache lives under corpus/profiles/, gitignored, persists
# across runs). Set FORCE=1 to re-pull from Common Crawl.
PINNED="corpus/profiles/scale-100m.jsonl"
if [ -z "${FORCE:-}" ] && [ -f "$PINNED" ]; then
  echo "cached seed already pinned at $PINNED ($(wc -l < "$PINNED") rows); skipping pull."
  echo "set FORCE=1 to re-pull from Common Crawl."
  if [ "$OUT" != "$PINNED" ]; then
    mkdir -p "$(dirname "$OUT")"
    cp "$PINNED" "$OUT"
    echo "copied cached seed -> $OUT"
  fi
  exit 0
fi

mkdir -p "$(dirname "$OUT")"
# Resume-friendly: if the raw pull file exists, keep appending rather than truncating,
# so a multi-day run that is interrupted does not start over. Set FRESH=1 to truncate.
[ -n "${FRESH:-}" ] && : > "$OUT"
[ -f "$OUT" ] || : > "$OUT"

# tld:count pairs, 10x the 10M build and spread across more TLDs so no single TLD has
# to carry a count past its distinct-host volume in the index. ~105M rows pre-dedup.
PULLS=(
  "dev:11000000"
  "app:9000000"
  "xyz:9000000"
  "page:7000000"
  "io:7000000"
  "me:7000000"
  "site:6000000"
  "online:6000000"
  "tech:6000000"
  "blog:5000000"
  "shop:5000000"
  "store:5000000"
  "cloud:4500000"
  "live:4500000"
  "world:4000000"
  "life:4000000"
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

# Pin: dedup to distinct URLs and freeze the result as the cached profile so the seed
# is never pulled again. The dedup sort is what the engine sees as distinct rows;
# corpusstat then writes the MANIFEST (records/urls/hosts/key-span) the same way
# scale-10m is pinned, so the 100M profile is a drop-in for the harness.
echo "[$(date +%H:%M:%S)] pinning distinct seed -> $PINNED"
mkdir -p "$(dirname "$PINNED")"
# -S gives sort a large buffer; 100M lines need on-disk merge, so point TMPDIR at a
# disk with room (the build dir, ~7 GB raw + temp).
TMPDIR="${TMPDIR:-$(dirname "$OUT")}" sort -S 4G -u "$OUT" > "$PINNED"
echo "  pinned $(wc -l < "$PINNED") distinct rows"

MAN="corpus/profiles/scale-100m.MANIFEST"
STAT="$(go run ./cmd/corpusstat "$PINNED" 2>/dev/null || true)"
{
  echo "profile=scale-100m"
  echo "crawl=${CRAWL}"
  echo "build=scripts/pull-corpus-100m.sh"
  echo "build_method=tld-scatter"
  echo "tld_pulls=${PULLS[*]}"
  echo "status_filter=200"
  echo "collapse=dedup-distinct-url"
  echo "corpus_file=$PINNED"
  echo "$STAT" | sed 's/^/# stat: /'
  echo "grouping=full-host"
  echo "built_by=ccrawl + scripts/pull-corpus-100m.sh"
} > "$MAN"
echo "  wrote $MAN"
echo "[$(date +%H:%M:%S)] cached seed ready: $PINNED (re-runs reuse it, no re-download)"
