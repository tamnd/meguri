---
title: "Running a crawl loop"
description: "Drain a frontier with meguri run: the staged loop, the logical clock, worker parallelism, and reading the result."
weight: 20
---

`meguri run` drives the staged engine loop over a checkpoint or a seed list. It admits work through the seen-set, orders it by importance, releases only what politeness allows, dispatches it to the fetcher, folds each outcome back in, and reschedules the URL on its freshness cycle, until the frontier drains. The result is written back as a `.meguri` checkpoint.

## Draining a checkpoint

Point `run` at a checkpoint built by [`seed`](/guides/seeding-a-frontier/) and give it somewhere to write the result:

```bash
meguri run -i frontier.meguri -o crawled.meguri
```

Or skip the separate seed step and run a fresh frontier straight from a URL list:

```bash
meguri run --seed urls.jsonl -o crawled.meguri
```

When it finishes, `meguri stats --data crawled.meguri` shows the per-status distribution and `meguri schedule --data crawled.meguri` lists what is due to come back around.

## The logical clock

By default the loop runs on a logical clock. Politeness delays are honoured in the order work is released, but the run does not actually wait the wall-clock seconds between fetches, so a large frontier drains as fast as the fetcher allows while still respecting one-fetch-per-host spacing. This is what makes a run reproducible and a test fast.

Pass `--wall` to honour real politeness waits, the mode a live crawl against the network uses:

```bash
meguri run -i frontier.meguri -o crawled.meguri --wall
```

## Worker parallelism

`--workers` sets the polite-host fetch parallelism, how many distinct hosts are in flight at once:

```bash
meguri run -i frontier.meguri -o crawled.meguri --workers 64
```

`0` (the default) picks a sensible value. Politeness is per host, so more workers means more hosts fetched concurrently, never the same host fetched faster.

## The fetcher

`run` drains against an offline fetcher built into the binary, which is what lets the whole loop run with no network in a test or a demo. In production the fetcher is [ami](https://github.com/tamnd/ami), bound through the `fetch.Fetcher` interface: the scheduler does not change, only what sits behind the dispatch call. The engine hands ami the next URL politeness allows and folds the outcome ami returns back into the frontier.

Next: [serve a durable partition](/guides/serving-a-partition/) so the frontier survives a restart.
