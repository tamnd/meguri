---
title: "Quick start"
description: "Seed a frontier from a URL list, drain it through the engine, and read the result back from a .meguri partition."
weight: 30
---

This walkthrough takes a list of URLs, turns it into a crawl frontier, drains that frontier through the engine, and reads the result back. It runs entirely offline against an embedded fetcher, so you can follow it without crawling the live web.

## 1. Get a list of URLs

meguri seeds from Common Crawl CDX records, the JSONL a `ccrawl search` produces. Any CDX JSONL works; here is one host's worth:

```bash
ccrawl search 'example.com/*' --limit 20000 -o jsonl > urls.jsonl
```

Each line is one record with a URL and its crawl metadata. If you do not have [ccrawl-cli](https://github.com/tamnd/ccrawl-cli), any tool that emits the same CDX JSONL shape will do.

## 2. Seed a frontier

`seed` reads those records, inserts each URL into a fresh frontier, and writes a `.meguri` checkpoint:

```bash
meguri seed -i urls.jsonl -o frontier.meguri
```

Or pipe straight from the search, no intermediate file:

```bash
ccrawl search 'example.com/*' --limit 20000 -o jsonl | meguri seed -o frontier.meguri
```

`--priority` sets the starting importance of every seeded URL (default `0.5`), and `--crawl-delay` sets the default per-host spacing in deciseconds (default `10`, so one second).

## 3. Drain it through the engine

`run` recovers the checkpoint, drives the staged engine loop to drain the frontier in priority-then-politeness order with the offline fetcher, and writes the post-run checkpoint:

```bash
meguri run -i frontier.meguri -o crawled.meguri
```

By default the loop runs on a logical clock, so politeness delays are honoured in ordering without making you wait in real time. Pass `--wall` for real waits, and `--workers N` to set the polite-host fetch parallelism.

## 4. Inspect the partition

A `.meguri` file describes itself. `inspect` reads its header and footer, which sit at the two ends of the file, and prints what the partition holds without decoding a single column of data:

```bash
meguri inspect crawled.meguri
```

```
meguri v1.0  partition 7
  file size      4126873 bytes
  hostkey range  0x0000000000000000 .. 0x3fffffffffffffff
  urls           182344
  hosts          4127
  url columns    23
  host columns   20
  checksum       crc32c
  default codec  zstd
  created        482817 epoch-hours
  flags          sorted|blob|schedule|seenset
  bytes/url      22.63
  next-due range 482820 .. 484992 epoch-hours
  regions:
    url_table    off=64         len=3981120
    host_table   off=3981184    len=140992
    schedule     off=4122176    len=2048
    seenset      off=4124224    len=512
    string_blob  off=4124736    len=4512
```

Every line comes from the header and the footer. The cost is two small reads, so `inspect` is as fast on a multi-gigabyte partition as on a tiny one.

## 5. See what is due, and the counters

`schedule` lists the URLs whose next crawl time has come around, read through the durable schedule index so it touches only the near buckets:

```bash
meguri schedule --data crawled.meguri --limit 20
```

`stats` prints the partition's counters, the footer summary for a file or the full per-status distribution for a live directory:

```bash
meguri stats --data crawled.meguri
```

## What you are looking at

- **partition** and **hostkey range** say which slice of the frontier this file owns. The router maps a host to exactly one partition by this range.
- **urls** and **hosts** are the row counts of the two tables. There are always far more URLs than hosts, which is why the host table stays resident while the URL table mostly lives on disk.
- **url columns** and **host columns** are the columnar schema: each field of a record is its own checksummed, paged column, so a reader touches only the columns it needs.
- **flags** name the regions present, here the sorted URL table, the string blob arena, the schedule index, and the seen-set filter.
- **bytes/url** is the on-disk cost of one frontier entry, the number the whole format is built to keep small as the crawl scales toward a hundred billion URLs.

## Next

Read the [file format](/reference/file-format/) page to see exactly what those regions and columns contain, the [CLI reference](/reference/cli/) for the full command surface, or the [guides](/guides/) for task-oriented walkthroughs like serving a durable partition and rebalancing files.
