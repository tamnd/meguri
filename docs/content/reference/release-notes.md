---
title: "Release notes"
description: "What changed in each meguri release."
weight: 40
---

The authoritative, commit-level history lives in [`CHANGELOG.md`](https://github.com/tamnd/meguri/blob/main/CHANGELOG.md) and on the [releases page](https://github.com/tamnd/meguri/releases). This page summarises each version.

## v0.1.0

The first release. meguri is a distributed web-crawler frontier and rescheduler: it turns a stream of discovered links into a polite, freshness-aware crawl schedule and serializes the whole frontier to `.meguri` partition files a fleet redistributes by moving files.

- **A crawl frontier with all three forces.** A URL is ordered by importance, released only when politeness allows (one in-flight fetch per host and per IP, spaced by a crawl delay), and rescheduled on a per-URL change-rate estimate so the crawl tracks the web instead of photographing it once. The three pull against each other and the engine resolves the conflict in that order.
- **A seen-set that holds work, not duplicates.** Discoveries fold in through canonicalisation and an approximate membership filter, so a link already known is dropped before it reaches the queue. The filter serializes into the partition so a reload does not rebuild it from scratch.
- **The staged engine loop.** `meguri seed` builds a frontier from a Common Crawl URL list; `meguri run` drains a checkpoint in priority-then-politeness order with the fetcher bound through a small interface; `meguri serve` opens a directory as a durable, log-structured partition that recovers on restart and checkpoints on shutdown.
- **The `.meguri` file.** One partition serializes to one self-describing, columnar, checksummed file: a 64-byte header, the URL and host tables, the schedule index, the seen-set filter, and a string arena, all bracketed by the magic `MEG1` and read from the tail in two small reads. Encoding is deterministic, so a checkpoint is diffable and a redistribution is verifiable byte for byte. `meguri inspect`, `schedule`, and `stats` read it; `pack` and `compact` write and consolidate it.
- **Fleet routing and projection.** `meguri map` reads a fleet manifest and routes a host through the partition map; `meguri bench` measures the real per-partition bytes/url and seen-set bits/url on a corpus slice and projects the cost to a hundred billion URLs against the named scaling walls.
- **Pure Go, packaged everywhere.** No cgo anywhere in the tree, `CGO_ENABLED=0`. The release ships archives for Linux, macOS, Windows, and FreeBSD on amd64 and arm64, `.deb`/`.rpm`/`.apk` packages, a multi-arch GHCR container image, checksums, SBOMs, a cosign signature, and Homebrew and Scoop entries, all from a single tag push.
