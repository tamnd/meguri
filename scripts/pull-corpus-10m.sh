#!/usr/bin/env bash
# Pull a ~10M-row realistic corpus from Common Crawl via ccrawl-cli, the 10x
# sibling of pull-corpus.sh. Same TLD-scatter shape (a few heavy hosts plus a long
# single-page tail across scatter TLDs), counts sized to reach ~10.5M rows before
# dedup, which collapses to ~10M distinct URLs. This is a multi-hour job: the CC
# index fetch runs at roughly 200 urls/s, so 10.5M rows is on the order of 14
# hours. Output is url-only JSONL, the host derived at seed time.
set -euo pipefail

OUT="${1:-corpus/build/wide-10m.jsonl}"
CRAWL="${CRAWL:-latest}"

# Cache: the pinned profile is the durable copy of the real seed. If it is already
# pinned, hand it back and skip the multi-hour Common Crawl pull entirely, so a
# re-run never re-downloads the seed (the cache lives under corpus/profiles/, which
# is gitignored and persists across runs). Set FORCE=1 to re-pull anyway.
PINNED="corpus/profiles/scale-10m.jsonl"
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
: > "$OUT"

# tld:count pairs, 10x the 1M build and spread wider so no single TLD has to carry
# a count past its distinct-host volume in the index.
PULLS=(
  "dev:1500000"
  "app:1200000"
  "xyz:1200000"
  "page:900000"
  "io:900000"
  "me:900000"
  "site:700000"
  "online:700000"
  "tech:700000"
  "blog:600000"
  "shop:600000"
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

# Pin: dedup to distinct URLs and freeze the result as the cached profile so the
# seed is never pulled again. The dedup sort is what the engine sees as distinct
# rows; corpusstat then writes the MANIFEST (records/urls/hosts/key-span/sha256) the
# same way scale-1m is pinned, so the 10M profile is a drop-in for the harness.
echo "[$(date +%H:%M:%S)] pinning distinct seed -> $PINNED"
mkdir -p "$(dirname "$PINNED")"
sort -u "$OUT" > "$PINNED"
echo "  pinned $(wc -l < "$PINNED") distinct rows"

# corpusstat prints the engine's view (rows/urls/hosts/hostkey range); fold that
# into a MANIFEST shaped like scale-1m.MANIFEST so the 10M profile is a drop-in.
MAN="corpus/profiles/scale-10m.MANIFEST"
STAT="$(go run ./cmd/corpusstat "$PINNED" 2>/dev/null || true)"
{
  echo "profile=scale-10m"
  echo "crawl=${CRAWL}"
  echo "build=scripts/pull-corpus-10m.sh"
  echo "build_method=tld-scatter"
  echo "tld_pulls=${PULLS[*]}"
  echo "status_filter=200"
  echo "collapse=dedup-distinct-url"
  echo "corpus_file=$PINNED"
  echo "$STAT" | sed 's/^/# stat: /'
  echo "grouping=full-host"
  echo "built_by=ccrawl + scripts/pull-corpus-10m.sh"
} > "$MAN"
echo "  wrote $MAN"
echo "[$(date +%H:%M:%S)] cached seed ready: $PINNED (re-runs reuse it, no re-download)"
