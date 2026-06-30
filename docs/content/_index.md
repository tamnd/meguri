---
title: "meguri"
description: "meguri (巡) is a distributed web-crawler frontier and rescheduler. It turns a stream of discovered links into a polite, freshness-aware crawl schedule for a frontier that scales to a hundred billion URLs, each partition a single .meguri file."
heroTitle: "Decide what to crawl next"
heroLead: "meguri is the decision layer of a crawl stack. It absorbs an endless stream of discovered links, deduplicates them, schedules them politely, keeps them fresh on a cycle, and serializes the whole frontier to compact .meguri partitions that a fleet redistributes by moving files."
heroPrimaryURL: "/getting-started/quick-start/"
heroPrimaryText: "Get started"
---

A crawler has three jobs: fetch pages, decide what to fetch next, and store what it found. meguri (巡, "to make the rounds and revisit on a cycle") is the middle one. It never opens a socket. It takes the links a fetcher discovered, decides which are worth crawling and in what order, holds back so a host is never hammered, and revisits pages on a schedule tuned to how often each one actually changes.

The state behind those decisions lives in `.meguri` files. A partition owns a range of hosts and serializes to exactly one file: a self-describing, columnar, checksummed container of every URL's and every host's crawl state. The same file is the engine's checkpoint, the unit a fleet redistributes by, and the cold archive.

Say you have a list of URLs from Common Crawl and you want a polite, ordered crawl plan out of it. One command builds a frontier; a second drains it; a third reads back what the partition holds:

```bash
ccrawl search '*.example.com/*' --limit 50000 -o jsonl | meguri seed -o frontier.meguri
meguri run -i frontier.meguri -o crawled.meguri
meguri inspect crawled.meguri
```

## What it does

- **One file is a whole partition.** A `.meguri` file holds the per-URL and per-host frontier state for one range of hosts, behind a CRC-checked header and a footer the reader finds from the tail. The tables are columnar and paged, so a tool reads a file's shape in two small reads no matter how large it is.
- **Deterministic on the byte.** The same partition value always encodes to the same bytes. A checkpoint is diffable, a redistribution is verifiable, and a round trip is exact.
- **Polite by construction.** A URL's identity is a 128-bit key whose high half is its host, so a host's URLs share a partition, a politeness bucket, and a contiguous range in the file. One in-flight fetch per host and per IP falls out of the layout.
- **Fresh on a cycle.** Every fetch outcome updates a per-URL change-rate estimate, so a page that changes hourly is revisited often and one that never changes drifts to the back.
- **Pure Go, one binary.** No cgo, no external queue, no database server. `CGO_ENABLED=0` builds for every platform.

## Where it fits

meguri is the scheduler between a fetcher and a store. A crawler like [ami](https://github.com/tamnd/ami) fetches the URLs meguri dispatches and returns outcomes; a corpus like [Common Crawl through ccrawl-cli](https://github.com/tamnd/ccrawl-cli) seeds the frontier and stands in for the live web in tests; a store and ranker like [tsumugi](https://github.com/tamnd/tsumugi) consumes the crawled pages.

## Where to go next

- New here? Start with the [introduction](/getting-started/introduction/), then the [quick start](/getting-started/quick-start/).
- Want to install it? See [installation](/getting-started/installation/).
- Looking for a specific task? The [guides](/guides/) cover seeding a frontier, running a crawl loop, serving a durable partition, rebalancing the files, and projecting the cost to fleet scale.
- Curious what is inside a partition? The [file format](/reference/file-format/) page documents the `.meguri` container, the [CLI reference](/reference/cli/) covers every command, and [configuration](/reference/configuration/) lists the environment and on-disk layout.
