---
title: "Configuration"
description: "The environment meguri reads, the durability dial, and the layout of a partition directory on disk."
weight: 20
---

meguri is configured through command-line flags (see the [CLI reference](/reference/cli/)). It reads no configuration file and almost no environment. The two things worth knowing beyond the flags are the durability dial that `serve` runs under and the layout of a partition directory on disk.

## Environment variables

| Variable | Read by | Meaning |
|----------|---------|---------|
| `MEGURI_CORPUS` | the test and bench suites | Path to a Common Crawl CDX JSONL slice. The corpus-backed gates and benchmarks skip unless this points at a real slice (see `scripts/fetch-corpus.sh`), so a build never fabricates a corpus. The shipped binary does not read it. |

The crawl-loop commands take their corpus from `--input`, `--seed`, or stdin, not from the environment, so a run is reproducible from its arguments alone.

## A partition directory

`meguri serve -d <dir>` opens a directory as a durable, log-structured partition store. The directory is the single-file promise in expanded form: open a directory, get a partition. It holds three things:

```
<dir>/
├── super              # two-slot superblock: which snapshot and log are current
├── snap-<gen>.meguri  # the latest checkpoint, a full .meguri partition file
└── log-<gen>          # the active append-only log since that checkpoint
```

- **The log is the journal.** Every crawl update is an append to `log-<gen>`, and that same log is its own recovery journal. There is no separate write-ahead log: the append-only log has nothing to roll back, so recovery is redo-only.
- **The snapshot is a real `.meguri` file.** A checkpoint folds the live state into `snap-<gen>.meguri`, the exact format any `meguri inspect` reads. Recovery loads that snapshot, rebuilds the resident index, and replays the log tail past the snapshot's frontier.
- **The superblock names what is current.** `super` is a two-slot record that commits a new (snapshot, log) generation atomically, so a crash mid-checkpoint always leaves a consistent prior generation to recover from.

A checkpoint rotates the generation: it writes `snap-<gen+1>.meguri` and a fresh `log-<gen+1>`, commits the superblock, then removes the superseded files. `meguri pack --data <dir>` writes a standalone `.meguri` from the live state without rotating, and `meguri stats --data <dir>` recovers the frontier to print its counters.

## The durability dial

`serve` runs the log under a three-position durability dial, the same dial the underlying log-structured store exposes. It trades device flushes for throughput:

| Position | When it flushes | What a power loss costs |
|----------|-----------------|-------------------------|
| None | never | everything the OS had not yet written back |
| Normal | at checkpoint boundaries | the updates since the last checkpoint, replayable from the log if the OS flushed it |
| Full | before every update returns, via group commit | nothing; one update is one device flush |

Normal is the default balance for a live crawl: a checkpoint is a durability boundary, and the log between checkpoints is replayed on recovery. Full is the fsync floor for when an acknowledged update must survive a power loss; None is the in-memory ceiling for a throwaway run or a benchmark.

## Residency

A partition can hold far more URLs than fit in memory. The store keeps the hot tail of the log resident and addresses the cold bulk by log offset on disk. `serve --resident-budget N` caps the number of resident URL records; `0` (the default) lets residency grow unbounded, which is the right choice when the partition fits in memory and the wrong one when it does not.
