#!/usr/bin/env bash
# Build the pinned scale-test corpus slices under corpus/profiles/.
#
# The profiles are deterministic head slices of the pinned ccrawl corpus
# (corpus/urls.jsonl, CC-MAIN-2026-25), so a rebuild is byte-identical. The 1M and
# 10M profiles need a larger pull than the pinned slice holds; fetch-corpus.sh with
# a wider domain set is the builder for those (scale spec doc 01).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$ROOT/corpus/urls.jsonl}"
OUT="$ROOT/corpus/profiles"

if [ ! -f "$SRC" ]; then
  echo "no corpus at $SRC; run scripts/fetch-corpus.sh first" >&2
  exit 1
fi

mkdir -p "$OUT"
total="$(wc -l < "$SRC")"
echo "source: $SRC ($total records)"

for n in 10000 100000; do
  name="scale-$((n/1000))k"
  head -n "$n" "$SRC" > "$OUT/$name.jsonl"
  echo "wrote $OUT/$name.jsonl ($(wc -l < "$OUT/$name.jsonl") records)"
done

echo "the full corpus ($SRC) is the 142k profile; 1M and 10M need a wider ccrawl pull (doc 01)"
