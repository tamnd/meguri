---
title: "Quick start"
description: "Inspect a .meguri partition and read its structure from the command line."
weight: 30
---

This walkthrough uses the M0 surface: the `.meguri` file and the `inspect` tool. The crawl-loop commands arrive with the engine in later milestones; when they do, they slot in ahead of this page.

## Inspect a partition

A `.meguri` file describes itself. `inspect` reads its header and footer, which sit at the two ends of the file, and prints what the partition holds without decoding a single column of data.

```bash
meguri inspect partition-00007.meguri
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
  flags          sorted|blob
  bytes/url      22.63
  next-due range 482820 .. 484992 epoch-hours
  regions:
    url_table    off=64         len=3981120
    host_table   off=3981184    len=140992
    string_blob  off=4122176    len=4512
```

Every line comes from the header and the footer. The cost is two small reads, so `inspect` is as fast on a multi-gigabyte partition as on a tiny one.

## What you are looking at

- **partition** and **hostkey range** say which slice of the frontier this file owns. The router maps a host to exactly one partition by this range.
- **urls** and **hosts** are the row counts of the two tables. There are always far more URLs than hosts, which is why the host table stays resident while the URL table mostly lives on disk.
- **url columns** and **host columns** are the columnar schema: each field of a record is its own checksummed, paged column, so a reader touches only the columns it needs.
- **checksum** and **default codec** are how the bytes are protected and packed. CRC32C guards every region and every page; the codec packs the column data.
- **bytes/url** is the on-disk cost of one frontier entry, the number the whole format is built to keep small as the crawl scales toward a hundred billion URLs.

## Next

Read the [file format](/reference/file-format/) page to see exactly what those regions and columns contain, or the [CLI reference](/reference/cli/) for the full command surface.
