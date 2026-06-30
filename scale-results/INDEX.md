# Scale results ledger

This is the append-only ledger of `meguri scale` runs (scale spec doc 10).
Each run writes a JSON result here and a CPU and heap profile per stage under `pprof/`.
A row is a smoke run unless it names a box of record; the laptop is the smoke box, never a number of record for a timed metric.
Deterministic size numbers (bytes/URL, bits/URL, FP rate) come from `meguri bench`, not this harness, and are box-independent.

## How to reproduce

```
scripts/build-profiles.sh          # build the pinned corpus slices under corpus/profiles/
make scale-smoke                   # run the 10k profile end to end (fast, catches breakage)
meguri scale -i corpus/urls.jsonl --profile 142k --commit $(git rev-parse --short HEAD)

# paired before/after for a seed-intake change: --seed-mode loop is the per-key
# baseline, --seed-mode batch (the default) is the DRUM batch path.
meguri scale -i corpus/urls.jsonl --profile 142k --seed-mode loop  --out scale-results/paired/loop
meguri scale -i corpus/urls.jsonl --profile 142k --seed-mode batch --out scale-results/paired/batch
```

## Runs

| date | profile | commit | box | seed mode | seed urls/cpu-s | seed alloc/URL | seed peak RSS | run urls/cpu-s | result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-30 | 10k | 38a0909 | laptop (smoke) | loop | 638.6k | 3501 B | 27 MiB | 715.6k | result.10k.38a0909.json |
| 2026-06-30 | 100k | 38a0909 | laptop (smoke) | loop | 170.8k | 1833 B | 124 MiB | 1.32M | result.100k.38a0909.json |
| 2026-06-30 | 142k | 38a0909 | laptop (smoke) | loop | 125.3k | 1717 B | 172 MiB | 1.22M | result.142k.38a0909.json |
| 2026-06-30 | 10k | a62abe1 | laptop (smoke) | loop | 751.2k | 3501 B | 28 MiB | 1.45M | paired/loop/result.10k.a62abe1.json |
| 2026-06-30 | 100k | a62abe1 | laptop (smoke) | loop | 166.1k | 1833 B | 124 MiB | 1.40M | paired/loop/result.100k.a62abe1.json |
| 2026-06-30 | 142k | a62abe1 | laptop (smoke) | loop | 121.7k | 1717 B | 172 MiB | 1.22M | paired/loop/result.142k.a62abe1.json |
| 2026-06-30 | 10k | a62abe1+batch | laptop (smoke) | batch | 841.7k | 4025 B | 27 MiB | 1.39M | paired/batch/result.10k.a62abe1.json |
| 2026-06-30 | 100k | a62abe1+batch | laptop (smoke) | batch | 847.9k | 2113 B | 136 MiB | 1.43M | paired/batch/result.100k.a62abe1.json |
| 2026-06-30 | 142k | a62abe1+batch | laptop (smoke) | batch | 766.1k | 2017 B | 184 MiB | 1.23M | paired/batch/result.142k.a62abe1.json |

## Findings

### F1: seed intake was O(n^2) on the exact-tier insert (FIXED)

Seed throughput fell more than fivefold from 10k to 142k (638.6k to 125.3k urls/cpu-s) while the run/drain stage stayed flat or improved.
The seed CPU profile at 142k showed 55.7 percent of CPU in `runtime.memmove` and 50.9 percent under `dedup.(*exactSet).add` via `SeenSet.Seen`.
That is the memmove-dominated insert into a sorted bucket, the O(n^2) signature of inserting n keys one at a time into a sorted slice.
The exact-tier bucket is keyed by HostKey, so a host's whole URL run lands in one bucket and each insert shifts half the bucket.

Fix (`frontier.SeedBatch` + `dedup.SeenSet.InsertBatch`): intake a window of seeds at once, check membership with the cheap binary-search `Contains`, and fold every new key into the seen-set in one DRUM merge per bucket instead of a shift per key.
The single-key `Seed` path is unchanged, so `Discover` and existing callers keep their semantics; `SeedBatch` only moves the insert off the hot path.
`TestSeedBatchMatchesSeedLoop` proves a frontier seeded by `SeedBatch` is byte-for-byte the one a `Seed` loop builds (identical checkpoint bytes and dispatch sequence), so the optimization changes cost, not behavior.

Paired before/after on the laptop smoke box (commit a62abe1, seed urls/cpu-s):

| profile | loop (before) | batch (after) | speedup |
| --- | --- | --- | --- |
| 10k | 751.2k | 841.7k | 1.1x |
| 100k | 166.1k | 847.9k | 5.1x |
| 142k | 121.7k | 766.1k | 6.3x |

The batch path holds flat near 840k across the range while the loop collapses, the O(n^2) to O(n log n) signature.
The seed CPU profile confirms it: total seed samples drop from 1050 ms (loop) to 170 ms (batch), `runtime.memmove` from 56.2 percent to 23.5 percent, and `exactSet.add` is gone, replaced by `exactSet.mergeBucket`; the memmove that remains is the checkpoint encoder (`format.writePage`), not the dedup insert.
The batch path trades a little memory for the speed: a bounded per-window key buffer and dedup map lift seed alloc/URL by roughly 300 to 500 bytes, flat in the window size, not the input size.

The numbers above are laptop smoke, good for the relative before/after; a box-of-record run restamps the absolute throughput.

```
go tool pprof -top scale-results/paired/batch/pprof/cpu.seed.142k.pprof
```
