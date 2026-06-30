# meguri

meguri (巡, to make the rounds and revisit on a cycle) is a distributed
web-crawler frontier and rescheduler written in Go.

It is the decision layer of a crawl stack. It turns an endless stream of
discovered links into a politely ordered, freshness-aware crawl schedule for a
frontier that scales to a hundred billion URLs across a fleet of shared-nothing
partitions. It sits between a fetcher (it dispatches work to one and consumes the
outcomes) and a store and ranker (it hands them crawled pages and importance
signals). It never opens a socket itself.

meguri has two embodiments: the engine that runs the crawl, and the `.meguri`
file that the engine checkpoints to, redistributes by, and archives as. A
partition owns a range of hosts and serializes to exactly one `.meguri` file: a
self-describing, columnar, checksummed container of the per-URL and per-host
crawl state.

## What it does

- **Absorbs discoveries and dedups them.** A stream of links from sitemaps and
  fetched pages folds into the frontier; a URL already known is dropped by an
  approximate seen-set, so the queue holds work, not duplicates.
- **Schedules politely.** A URL's 128-bit key carries its host in the high half,
  so a host's URLs share a partition and a politeness bucket. At most one fetch
  per host and per IP is ever in flight, spaced by a crawl delay.
- **Keeps the crawl fresh.** Every fetch outcome updates a per-URL change-rate
  estimate, so a page that changes hourly comes back often and one that never
  changes drifts to the back.
- **One file is a whole partition.** The frontier serializes to a deterministic,
  columnar `.meguri` file behind a CRC-checked header and a tail footer, so a
  fleet rebalances by moving files and a tool reads a file's shape in two reads.
- **Pure Go, one binary.** No cgo, no external queue, no database server.
  `CGO_ENABLED=0` builds for every platform, and CI proves it.

## Quick start

```bash
# Build a frontier from a Common Crawl URL list, then drain it.
ccrawl search '*.example.com/*' --limit 50000 -o jsonl \
  | meguri seed -o frontier.meguri
meguri run -i frontier.meguri -o crawled.meguri

# Read what a partition holds without decoding a column.
meguri inspect crawled.meguri

# See what is due to be crawled next.
meguri schedule --data crawled.meguri --limit 20
```

The full command surface (`seed`, `run`, `serve`, `inspect`, `schedule`,
`stats`, `map`, `pack`, `compact`, `bench`) is documented in the
[CLI reference](https://meguri.tamnd.com/reference/cli/).

## The `.meguri` file

A `.meguri` file is bracketed by the magic `MEG1` at both ends. It opens with a
fixed 64-byte header and closes with a footer the reader finds from the tail, so
a tool learns a file's shape in two small reads regardless of its size. Between
them sit five regions: the URL table, the host table, the schedule index, the
seen-set filter, and a shared string arena, each column framed into checksummed
pages. Every checksum is CRC32C by default.

Encoding is deterministic: the same partition value always produces the same
bytes, which is what makes a checkpoint diffable and a redistribution verifiable.

## Build

```bash
make build      # builds bin/meguri, CGO disabled
make test       # full suite with the race detector
```

The build is pure Go with `CGO_ENABLED=0`. There is no cgo anywhere in the tree.

## Install

Release archives, Linux packages (deb, rpm, apk), a container image on GHCR, and
Homebrew and Scoop entries are produced by a single tag push. See the
[documentation](https://meguri.tamnd.com) for the install matrix.

## License

MIT. See [LICENSE](LICENSE).
