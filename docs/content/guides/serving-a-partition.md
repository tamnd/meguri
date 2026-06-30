---
title: "Serving a durable partition"
description: "Open a directory as a log-structured partition that recovers on restart, with meguri serve, the durability dial, and a resident budget."
weight: 30
---

`meguri run` drains a checkpoint in one shot. `meguri serve` is the long-lived form: it opens a directory as a durable, log-structured partition, drives the same staged loop, and survives a restart. A crawl update is an append to the log, the log is its own recovery journal, and a checkpoint folds the live state into a `.meguri` snapshot. Open the directory again and the partition recovers exactly where it left off.

## Opening a partition

Give `serve` a directory. On a fresh directory, seed it from a URL list; on an existing one, it recovers:

```bash
# First run: create and seed the partition.
meguri serve -d ./part-7 --seed urls.jsonl

# Later runs: recover and continue. No --seed needed.
meguri serve -d ./part-7
```

The directory holds the active log, the latest `.meguri` snapshot, and a two-slot superblock that names which generation is current. The [configuration](/reference/configuration/) page documents the layout. Recovery loads the snapshot, rebuilds the index, and replays the log tail past it, redo-only, since an append-only log has nothing to roll back.

## Choosing durability

`serve` runs the log under a three-position durability dial that trades device flushes for throughput. Normal, the default, makes a checkpoint a durability boundary and replays the log between checkpoints on recovery, which is the right balance for a live crawl. Use Full when an acknowledged update must survive a power loss, and None for a throwaway run where speed is all that matters. The dial and what each position costs on a power loss are documented in [configuration](/reference/configuration/#the-durability-dial).

## Capping residency

A partition can hold more URLs than fit in memory. `serve` keeps the hot tail of the log resident and addresses the cold bulk on disk by log offset. `--resident-budget` caps the resident URL records:

```bash
meguri serve -d ./part-7 --resident-budget 20000000
```

`0` (the default) lets residency grow unbounded, correct when the partition fits in memory. Set a budget when it does not, so the resident set stays within RAM and the rest stays on disk.

## Reporting where it routes

Pass `--manifest` and `serve` reports where this partition's host-key range sits in the fleet map, the same map [`meguri map`](/guides/rebalancing-files/) prints:

```bash
meguri serve -d ./part-7 --manifest meguri.manifest
```

## Checkpointing on shutdown

On a clean shutdown `serve` folds the live state into a fresh snapshot and rotates the generation, so the next open recovers from the snapshot with a short log tail to replay. You can also take a standalone checkpoint at any time without stopping the server with [`meguri pack`](/guides/rebalancing-files/), which reads the directory read-only and never mutates the live partition.

Next: [rebalance the files](/guides/rebalancing-files/) when a partition runs hot or cold.
