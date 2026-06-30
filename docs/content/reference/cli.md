---
title: "CLI reference"
description: "Every command and flag the meguri binary exposes."
weight: 30
---

The `meguri` binary is the front door to the frontier engine and its files. The crawl-loop commands (`seed`, `run`, `serve`) drive the engine; the file commands (`inspect`, `schedule`, `stats`, `map`, `pack`, `compact`) read and reshape `.meguri` partitions; `bench` projects the cost to fleet scale.

## meguri

```
meguri [command] [--flags]
```

Run with no command for the help screen. Global flags:

| Flag | Meaning |
|------|---------|
| `-v`, `--version` | Print the version, commit, and build date. |
| `-h`, `--help` | Print help for the binary or any subcommand. |

## meguri seed

Build a `.meguri` checkpoint from a CDX JSONL list of URLs. `seed` reads Common Crawl CDX records (`ccrawl search ... -o jsonl`) from `--input` or stdin, inserts each URL into a fresh frontier, and writes the checkpoint to `--out`.

```bash
meguri seed -i urls.jsonl -o frontier.meguri
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-i`, `--input` | stdin | CDX JSONL file to read. |
| `-o`, `--out` | | Path to write the `.meguri` checkpoint. |
| `--priority` | `0.5` | Initial priority for every seeded URL. |
| `--crawl-delay` | `10` | Default per-host crawl delay, in deciseconds. |

## meguri run

Drive the frontier engine over a checkpoint or seed list. `run` loads a partition (`--input` a `.meguri` checkpoint or `--seed` a CDX JSONL list), drives the staged engine loop to drain it in priority-then-politeness order with the offline fetcher, and writes the result to `--out`. The production fetcher is [ami](https://github.com/tamnd/ami), bound through the `fetch.Fetcher` interface.

```bash
meguri run -i frontier.meguri -o crawled.meguri
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-i`, `--input` | | `.meguri` checkpoint to recover and run. |
| `--seed` | | CDX JSONL seed list to run a fresh frontier from. |
| `-o`, `--out` | | Path to write the post-run `.meguri` checkpoint. |
| `--priority` | `0.5` | Initial priority for seeded URLs. |
| `--crawl-delay` | `10` | Default per-host crawl delay, in deciseconds. |
| `--workers` | `0` | Polite-host fetch parallelism (`0` = default). |
| `--wall` | off | Use a wall clock (real politeness waits) instead of the logical clock. |

## meguri serve

Open a directory as a durable partition and drive its crawl loop. `serve` opens `--dir` as a log-structured partition store, recovers its frontier (seeding from `--seed` on a fresh directory), drives the staged engine loop with the offline fetcher, and checkpoints back on shutdown. `--manifest` reads a fleet catalog and reports where this partition's range routes.

```bash
meguri serve -d ./part-7 --seed urls.jsonl
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-d`, `--dir` | *(required)* | Partition store directory to open or create. |
| `--seed` | | CDX JSONL seed list to load into a fresh partition. |
| `--manifest` | | Fleet manifest to report this partition's routing against. |
| `--resident-budget` | `0` | Maximum resident URL records (`0` = unbounded). |
| `--priority` | `0.5` | Initial priority for seeded URLs. |
| `--crawl-delay` | `10` | Default per-host crawl delay, in deciseconds. |
| `--workers` | `0` | Polite-host fetch parallelism (`0` = default). |
| `--wall` | off | Use a wall clock (real politeness waits) instead of the logical clock. |

## meguri inspect

Print the structure and stats of a `.meguri` file: the header facts, the region layout, the column counts, the checksum and codec, and the at-a-glance stats. The summary is computed from the header and the footer, so the cost is two small reads regardless of file size.

```bash
meguri inspect crawled.meguri
```

`inspect` takes the file path as its only argument and no flags beyond `-h`. The output is documented field by field on the [quick start](/getting-started/quick-start/) page.

## meguri schedule

Show what is due to be crawled, by due time. With `--data` a directory, `schedule` recovers the live frontier and prints each due URL with its canonical string and due hour. With `--data` a `.meguri` file, it reads cold through the durable schedule index (the timing wheel, when present) so it touches only the near buckets, not the whole frontier.

```bash
meguri schedule --data crawled.meguri --limit 20
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--data` | *(required)* | Partition directory or `.meguri` file to read. |
| `--before` | `0` | Due-time horizon in epoch-hours (`0` = now). |
| `--host` | | Filter to one host key (hex `0x...` or decimal). |
| `--limit` | `50` | Maximum URLs to list (`0` = all). |

## meguri stats

Print the counters of a partition directory or a `.meguri` file. A directory recovers the live frontier and prints the full per-status distribution, the pending and due counts, and the seen-set occupancy; a single file prints the footer summary (URL and host counts, due range, region presence) without recovery.

```bash
meguri stats --data crawled.meguri
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--data` | *(required)* | Partition directory or `.meguri` file to read. |

## meguri map

Print the partition map from a fleet manifest. `map` reads a `meguri.manifest` catalog (`--manifest`) and prints each partition's host-key range, URL and host counts, bytes/url, and epoch, then whether the ranges tile the key space cleanly. With `--host` it routes that single host through the map and prints its owning partition.

```bash
meguri map --manifest meguri.manifest
meguri map --manifest meguri.manifest --host 0x3fffffffffffffff
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--manifest` | *(required)* | `meguri.manifest` catalog to read the map from. |
| `--host` | | Resolve a single host key (hex `0x...` or decimal) through the map. |

## meguri pack

Write a partition's live state to a fresh `.meguri` file, the explicit checkpoint command. `--data` is the partition directory to read; `--out` is where to write the file. The directory is opened read-only and dropped without a checkpoint, so packing never mutates the live partition.

```bash
meguri pack --data ./part-7 --out part-7.meguri
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--data` | *(required)* | Partition directory to read. |
| `--out` | `<data>/pack.meguri` | Path to write the `.meguri` file. |

## meguri compact

Merge `.meguri` files, re-run the cascade, GC tombstones. `compact` merges its inputs into one partition (consolidation, the file side of rebalancing), re-runs the columnar cascade so the file packs to tens of bytes per URL, and with `--gc` drops the Gone tombstones past their re-probe horizon and reclaims the string arena. Inputs must own disjoint, ordered host-key ranges; an overlap is reported rather than producing a file a reader would reject.

```bash
meguri compact part-7.meguri part-8.meguri --out merged.meguri --gc
```

| Flag | Default | Meaning |
|------|---------|---------|
| `<file...>` | *(required)* | One or more `.meguri` files to merge. |
| `--out` | `compact.meguri` | Path to write the compacted file (by the first input). |
| `--gc` | off | Garbage-collect Gone tombstones and reclaim the string arena. |

## meguri bench

Measure per-partition costs on a corpus slice and project to 100B URLs. `bench` reads CDX records from `--input` or stdin, builds a real partition, measures the deterministic `.meguri` bytes/url and seen-set bits/url with its achieved false-positive rate, and prints the fleet projection as measured-times-count against the three named scaling walls.

```bash
meguri bench -i urls.jsonl
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-i`, `--input` | stdin | CDX JSONL file to read. |
| `--priority` | `0.5` | Initial priority for every seeded URL. |
| `--crawl-delay` | `10` | Default per-host crawl delay, in deciseconds. |
| `--total-urls` | `1e11` | Fleet total URL count to project to. |
| `--urls-per-partition` | `3e7` | Per-partition capacity, the projection lever. |
| `--rebalance-to` | `16` | Partitions to grow the slice to for the rebalance-vs-bandwidth arm. |
| `--rebalance-bw` | `1200` | Device read bandwidth in MB/s the shipped bytes are divided by. |
| `--scheduler-sel-rate` | `1e6` | Measured scheduler selections/s to report the politeness ceiling against. |

## meguri completion

Generate a shell autocompletion script for bash, zsh, fish, or PowerShell.

```bash
meguri completion zsh > "${fpath[1]}/_meguri"
```
