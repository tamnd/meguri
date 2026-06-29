# meguri

meguri (巡, to make the rounds and revisit on a cycle) is a distributed
web-crawler frontier and rescheduler written in Go.

It is the decision layer of a crawl stack. It turns an endless stream of
discovered links into a politely ordered, freshness-aware crawl schedule for a
frontier that scales to a hundred billion URLs across a fleet of shared-nothing
partitions. It sits between a fetcher (it dispatches work to one and consumes the
outcomes) and a store and ranker (it hands them crawled pages and importance
signals).

meguri has two embodiments: the engine that runs the crawl, and the `.meguri`
file that the engine checkpoints to, redistributes by, and archives as. A
partition owns a range of hosts and serializes to exactly one `.meguri` file: a
self-describing, columnar, checksummed container of the per-URL and per-host
crawl state.

## Status

This is M0: the data model, the `.meguri` file format, and the `inspect` tool,
on a pure-Go build with the full fleet CI and release pipeline. The crawl engine
(the frontier, the seen-set, politeness, freshness, prioritization, the durable
store, and the multi-partition router) lands milestone by milestone after it.

## The `.meguri` file

A `.meguri` file is bracketed by the magic `MEG1` at both ends. It opens with a
fixed 64-byte header and closes with a footer the reader finds from the tail, so
a tool learns a file's shape in two small reads regardless of its size. Between
them sit columnar regions: the URL table, the host table, and a shared string
arena, each column framed into checksummed pages. Every checksum is CRC32C by
default.

Encoding is deterministic: the same partition value always produces the same
bytes, which is what makes a checkpoint diffable and a redistribution
verifiable.

```
meguri inspect partition-00007.meguri
```

prints the header facts, the region layout, the column counts, and the
at-a-glance stats without decoding a single column of data.

## Build

```
make build      # builds bin/meguri, CGO disabled
make test       # full suite with the race detector
```

The build is pure Go with `CGO_ENABLED=0`. There is no cgo anywhere in the tree,
and CI proves it.

## Install

Release archives, Linux packages (deb, rpm, apk), a container image on GHCR, and
Homebrew and Scoop entries are produced by a single tag push. See the
[documentation](https://meguri.tamnd.com) for the install matrix.

## License

MIT. See [LICENSE](LICENSE).
