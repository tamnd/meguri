---
title: "File format"
description: "The on-disk layout of a .meguri partition: magic, header, columnar regions, paged columns, and the tail footer."
weight: 10
---

A `.meguri` file is the serialized form of one frontier partition. It is self-describing, columnar, and checksummed end to end, and it is laid out so a reader learns its structure from two small reads at the two ends of the file.

## At a glance

```
+--------+----------------------------------------------------+
| MEG1   |  header (64 bytes)                                 |
+--------+----------------------------------------------------+
|        |  url table region    (columns, paged)             |
|        |  host table region   (columns, paged)             |
|        |  string/blob region  (the string arena)           |
+--------+----------------------------------------------------+
|        |  footer (region directory, column directories,    |
|        |          stats, string metadata)                  |
+--------+----------------------------------------------------+
| footer_length u32 | footer_crc32c u32 | MEG1                |
+--------+----------------------------------------------------+
```

The magic `MEG1` brackets the file at both ends. The header is a fixed 64 bytes at offset 0. The footer sits just before the 12-byte trailer, and the trailer's `footer_length` lets a reader seek straight to it from the end.

## The header

The 64-byte header carries the global facts a reader needs before it parses anything else: the format version, the partition id, the range of host keys this partition owns, the row counts of the two tables, the offset of the footer, and the creation time. Its last four bytes are a CRC32C over the first sixty, so a truncated or corrupt header is caught immediately.

## The URL key

Every URL is identified by a 128-bit key: the high 64 bits are the host key, the low 64 bits are the path key. The URL table is sorted by this key read as a big-endian integer, so all of a host's URLs form one contiguous run. The host key is at once the partition key, the politeness key, and the colocation key, which is why a host's URLs always travel together.

## Regions

Between the header and the footer sit the data regions, in a fixed order:

- **URL table.** One column per field of the per-URL record: the key halves, the crawl status, the priority, the timestamps, the change-rate estimate, the content fingerprints, and so on. There are far more URLs than anything else, so this is the largest region and the one designed to stay on disk.
- **Host table.** One column per field of the per-host record: the resolved IP, the robots state, the politeness buckets, the per-host budgets, and the imported quality signal. There are few hosts, so this region stays resident.
- **String/blob region.** A shared arena of the variable-length strings the fixed-width columns point into by offset: canonical URLs, host names, ETags. A record stores an offset, not the bytes, so the fixed columns stay fixed-width and packable.

Each region's bounds and CRC32C live in the footer's region directory, so the reader verifies a whole region with one checksum before trusting any column in it.

## Columns and pages

A column is a sequence of values of one field, stored column-major and framed into pages. Each page has a 32-byte header with its value count, its uncompressed and compressed sizes, and a CRC32C over its payload, so corruption is localized to a page rather than a whole column. After encoding, a page's payload is run through a block codec; CRC32C is the default checksum and zstd the default codec, both chosen so encoding is deterministic.

The footer's column directory locates each column's pages and summarizes the column: its value count, its encoding and codec, its checksum, and for the integer columns a min and max zone map so a reader can skip a column whose range cannot match a predicate.

## The footer

The footer is a sequence of length-prefixed sections: the region directory, the URL and host column directories, a stats block, and an optional string-metadata section. Unknown sections are skipped by length, so a newer writer's additions do not break an older reader. The footer is found from the tail: the last twelve bytes are `footer_length`, a CRC32C over the footer, and the closing magic.

## Determinism

Encoding is deterministic. The same partition value always produces the same bytes: there is no map iteration in the footer, the creation time is supplied rather than read from the clock, and the zstd encoder is fixed. This is what makes a checkpoint diffable, a redistribution verifiable, and the round-trip test meaningful: decode a file, re-encode it, and you get the same bytes back.
