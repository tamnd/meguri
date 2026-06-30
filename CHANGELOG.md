# Changelog

All notable changes to meguri are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.1.0

The first release: a distributed web-crawler frontier and rescheduler that turns
a stream of discovered links into a polite, freshness-aware crawl schedule and
serializes the frontier to `.meguri` partition files.

### Added

- The crawl frontier: importance ordering, per-host and per-IP politeness with a
  crawl delay, and per-URL freshness rescheduling from a change-rate estimate.
- The seen-set: URL canonicalisation plus an approximate membership filter that
  drops known links before they reach the queue, serialized into the partition.
- The staged engine loop, driven by `meguri seed`, `meguri run`, and
  `meguri serve`, with the fetcher bound through the `fetch.Fetcher` interface.
- The durable, log-structured partition store: an append-only log as its own
  recovery journal, a three-position durability dial, larger-than-memory
  residency, and a `.meguri` checkpoint with a two-slot superblock.
- The `.meguri` file format: a 64-byte header, the URL and host tables, an
  optional schedule index and seen-set filter, and a string arena, columnar and
  CRC32C-checked, bracketed by the magic `MEG1` and read from the tail.
  Deterministic encoding, so a checkpoint is diffable and a redistribution is
  verifiable byte for byte.
- The file and fleet tools: `meguri inspect`, `schedule`, `stats`, `map`,
  `pack`, `compact`, and `bench`.
- Packaging from a single tag push: archives, `.deb`/`.rpm`/`.apk`, a multi-arch
  GHCR image, checksums, SBOMs, a cosign signature, and Homebrew and Scoop
  entries. Pure Go, `CGO_ENABLED=0`, no cgo anywhere in the tree.
