---
title: "Seeding a frontier"
description: "Turn a Common Crawl URL list into a .meguri checkpoint, and choose the starting priority and crawl delay."
weight: 10
---

A crawl starts from a set of URLs. meguri reads them as Common Crawl CDX records, the JSONL a `ccrawl search` produces, and folds each one into a fresh frontier. The result is a `.meguri` checkpoint you can run, inspect, or hand to a fleet.

## From a search to a checkpoint

The simplest path pipes a search straight into `seed`:

```bash
ccrawl search 'example.com/*' --limit 50000 -o jsonl | meguri seed -o frontier.meguri
```

If you already have the records on disk, read them with `-i`:

```bash
meguri seed -i urls.jsonl -o frontier.meguri
```

`seed` deduplicates as it goes: a URL that canonicalises to one already in the frontier is dropped, so the checkpoint holds distinct work even if the input repeats. When it finishes, `meguri inspect frontier.meguri` shows the URL and host counts and the host-key range the partition covers.

## Setting the starting priority

Every seeded URL enters with the same starting importance, `--priority` (default `0.5`, on a 0-to-1 scale):

```bash
meguri seed -i urls.jsonl -o frontier.meguri --priority 0.8
```

The priority is the importance signal the engine orders on, within the set of URLs politeness currently allows. Seed a high-value list higher so it drains ahead of a broad background crawl folded in later.

## Setting the crawl delay

`--crawl-delay` is the default per-host spacing, in deciseconds (default `10`, so one second):

```bash
meguri seed -i urls.jsonl -o frontier.meguri --crawl-delay 30
```

This is the floor the engine applies before it learns a host's own rate from robots and from how the host responds. Raise it to be gentler on a fragile site; the per-host value the engine derives at run time can only make a host slower, never faster than this floor.

## What you have

A `.meguri` checkpoint is a complete, self-describing partition. You can:

- drain it with [`meguri run`](/guides/running-a-crawl-loop/),
- read its shape with `meguri inspect`,
- list what is due with `meguri schedule --data frontier.meguri`,
- or measure its per-URL cost with [`meguri bench`](/guides/projecting-to-scale/).

Next: [run a crawl loop](/guides/running-a-crawl-loop/).
