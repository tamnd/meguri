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
```

## Runs

| date | profile | commit | box | seed urls/cpu-s | seed alloc/URL | seed peak RSS | run urls/cpu-s | result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-30 | 10k | 38a0909 | laptop (smoke) | 638.6k | 3501 B | 27 MiB | 715.6k | result.10k.38a0909.json |
| 2026-06-30 | 100k | 38a0909 | laptop (smoke) | 170.8k | 1833 B | 124 MiB | 1.32M | result.100k.38a0909.json |
| 2026-06-30 | 142k | 38a0909 | laptop (smoke) | 125.3k | 1717 B | 172 MiB | 1.22M | result.142k.38a0909.json |

## Findings

### F1: seed intake is O(n^2) on the exact-tier insert (open)

Seed throughput falls more than fivefold from 10k to 142k (638.6k to 125.3k urls/cpu-s) while the run/drain stage stays flat or improves.
The seed CPU profile at 142k (`pprof/cpu.seed.142k.pprof`) shows 55.7 percent of CPU in `runtime.memmove` and 50.9 percent under `dedup.(*exactSet).add` via `SeenSet.Seen`.
That is the memmove-dominated insert into a sorted bucket, the O(n^2) signature of inserting n keys one at a time into a sorted slice.
The exact-tier bucket is keyed by HostKey, so a host's whole URL run lands in one bucket and each insert shifts half the bucket.

Fix: route seed intake through the DRUM batch `SeenSet.Merge` (the code's own scale path) instead of per-key `Seen`, so each bucket is sorted once and merged in a single pass.
`dedup/drum.go` documents `merge` as the scale path and `add` as the single-key path.
The next iteration makes the fix and records the paired before/after on a box of record; the 142k seed row above is the "before".

```
go tool pprof -top -cum scale-results/pprof/cpu.seed.142k.pprof
```
