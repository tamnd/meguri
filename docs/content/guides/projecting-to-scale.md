---
title: "Projecting to fleet scale"
description: "Measure the real per-partition cost on a corpus slice with meguri bench, and project it to a hundred billion URLs against the named scaling walls."
weight: 50
---

The whole format exists to keep one frontier entry small, because the frontier scales to a hundred billion URLs and the per-URL cost multiplies by that count. `meguri bench` is how you check the claim on real data: it builds a real partition from a corpus slice, measures the deterministic `.meguri` bytes/url and the seen-set bits/url with the false-positive rate it actually achieves, and projects those measured numbers to the full fleet against three named walls.

## Measuring a slice

Feed `bench` the same CDX JSONL `seed` takes:

```bash
ccrawl search '*.example.com/*' --limit 100000 -o jsonl | meguri bench
```

It builds the partition, measures it, and prints the per-partition cost and the fleet projection. The measurement is the deterministic file, not an estimate: the bytes/url it reports is the bytes/url a `meguri inspect` of the same partition would show.

## The projection levers

Two flags set the size of the fleet you project onto:

```bash
meguri bench -i urls.jsonl --total-urls 1e11 --urls-per-partition 3e7
```

- `--total-urls` (default `1e11`) is the fleet's total URL count, the number the measured per-URL cost multiplies by.
- `--urls-per-partition` (default `3e7`) is the per-partition capacity, so `total-urls / urls-per-partition` is the partition count, the lever that sets how many files the fleet holds.

The projection is measured-times-count: the bytes/url from the real slice times the total URL count gives the frontier's on-disk footprint, and the partition count tells you how many `.meguri` files that is.

## The three walls

`bench` reports the projection against three named ceilings, the things that actually bound a frontier at scale rather than a made-up benchmark:

- **The storage wall.** The measured bytes/url times `--total-urls` is the frontier's total size. This is what the columnar cascade is fighting to keep small.
- **The rebalance wall.** `--rebalance-to` grows the slice to that many partitions and divides the shipped bytes by `--rebalance-bw` (a device read bandwidth in MB/s, the named wall, not a measured disk) to bound how long moving a partition takes.
- **The politeness wall.** `--scheduler-sel-rate` is the measured scheduler selection rate the projection reports the politeness ceiling against: how many URLs a second the scheduler can release across the fleet without breaking per-host spacing.

```bash
meguri bench -i urls.jsonl \
  --rebalance-to 16 --rebalance-bw 1200 --scheduler-sel-rate 1e6
```

Each wall turns a per-partition measurement into a fleet-level limit, so a slice you can hold on one machine tells you what the whole crawl will cost.

## Why it is trustworthy

`bench` refuses to run on a fabricated corpus: the corpus-backed measurement reads a real CDX slice (the gates skip unless `MEGURI_CORPUS` points at one), and the bytes it reports come from the deterministic encoder, so two runs on the same slice agree to the byte. A projection is only as honest as its measurement, and this one is measured, not modelled.
