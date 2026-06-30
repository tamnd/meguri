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
| 2026-06-30 | 1m (wide) | 569a53a | laptop (smoke) | batch | 708.8k | 2101 B | 1.24 GiB | 592.7k | wide1m/result.1m.569a53a.json |

The 1m (wide) row is the first run on the realistic multi-TLD corpus: `corpus/build/wide.dedup.jsonl`, 1,053,092 distinct URLs over 64,554 hosts (16.31 urls/host, Zipfian), pulled by `scripts/pull-corpus.sh` across six single-page-heavy TLDs (.dev/.app/.xyz/.page/.io/.me). Earlier rows used the narrow 11-host github-concentrated corpus. The shape and throughput numbers are laptop smoke (relative only); the deterministic size anchors (bytes/url, bits/url) are box-independent and reported under F2 and in doc 14.

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

### F2: the F1 batch fix holds flat to 1M, and resident bytes/url is stable across scale

Two things to confirm at 1M on the realistic corpus. First, that the O(n log n) batch seed path from F1 does not collapse at 10x the largest profile it was tuned against. Second, that the resident-memory cost per URL, the number doc 12 extrapolates to the 100M uncapped falsifier, is stable as the URL count grows and not an artifact of the small profiles.

Seed throughput at 1M (wide corpus) is 708.8k urls/cpu-s, essentially the 766.1k the batch path held at 142k. The loop path would have fallen below 50k here on its O(n^2) curve; the batch path stays in its flat band, so the F1 fix scales through 1M. Run/drain holds at 592.7k urls/cpu-s with 0 GC cycles, the same drain shape as the smaller profiles.

Resident bytes per URL is the load-bearing number. Peak RSS at the end of seed is 1327972352 bytes over 1053092 distinct URLs, which is 1260.8 resident bytes/url. Doc 12 §3 derived 1358.7 bytes/url from the 142k seed (184 MiB / 142k) and used it for the 100M uncapped extrapolation of about 136 GB. The 1M measurement lands at 1260.8, within 7.5 percent of that derivation across a 7x jump in scale. The per-url resident cost is therefore stable, not a small-profile artifact, and the 136 GB uncapped-at-100M falsifier holds (1260.8 B/url x 100M is 117 GB, still far past any server2/server3 box, the same conclusion). This is the empirical backing for doc 12's claim that the residency model, not raw resident slices, is what a single box needs to clear 100M.

The deterministic size anchors from the matching bench run on the same corpus (box-independent): 24.58 bytes/url on disk, 11.02 seen-set bits/url, 0.82 percent false-positive rate, 0 false negatives over 1,049,819 URLs / 64,554 hosts. These reproduce the per-partition numbers doc 14 carries and confirm the wider host mix shifts bytes/url only slightly (24.58 vs 25.05 on the narrow corpus), exactly the host-mix drift doc 01 predicted.

```
go tool pprof -top scale-results/wide1m/pprof/cpu.seed.1m.pprof
```

### F3: the read path is cheap, decode runs ~6x faster than encode and near-allocation-free

The harness measured seed (build a frontier) and run (drain it) but never read a checkpoint back, so the whole disk-read side of the ledger was zero. The inspect stage closes that: it reads the `.meguri` the seed stage wrote off disk and decodes every column (zstd, FSST, the urlkey and host columns), the cold-restore cost a serve or recovery stage pays before it can do anything.

On the 1M wide corpus (commit 65ea059, laptop smoke, `scale-results/wide1m-io/`):

| stage | wall | urls/s wall | peak heap | alloc/url | mallocs | disk |
| --- | --- | --- | --- | --- | --- | --- |
| seed (encode) | 1.4635 s | 719.6k | 1.39 GiB | 2101 B | 1,205,390 | 24.61 MiB written |
| inspect (decode) | 0.2458 s | 4.27M | 496 MiB | 728 B | 330 | 24.61 MiB read |

Decode is 5.9x faster than encode in wall time and reconstructs all 1,049,819 URLs from a 25,805,603-byte file, which is 24.58 bytes/url read, exactly the deterministic on-disk anchor the bench reports. The striking number is mallocs: the decode allocates 330 objects total, against the 1.2 million the seed allocates, because the columnar reader bulk-allocates each column once and slices into it rather than building a struct per URL. Peak heap during decode is 496 MiB against the seed's 1.39 GiB.

The consequence for the larger goal: a cold restore of a 1M partition costs about a quarter second and half a gigabyte of heap on the smoke box, so the recovery and serve stages doc 07 builds on start from a cheap read, not an expensive rebuild. The number to watch as the profiles grow is whether decode stays near-allocation-free; the 330-malloc figure is the regression anchor. Box-of-record restamps the absolute wall time.

```
go tool pprof -top scale-results/wide1m-io/pprof/cpu.inspect.1m.pprof
```
