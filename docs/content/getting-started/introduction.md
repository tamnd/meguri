---
title: "Introduction"
description: "Why a crawl needs a frontier, what meguri decides, and how a partition becomes a single .meguri file."
weight: 10
---

A web crawl is not a download. It is a scheduling problem wearing a network costume. At any moment a crawler knows about far more URLs than it can fetch, and most of what it knows it has already seen. The hard part is not fetching a page; it is deciding which page to fetch next, when to come back, and how to do all of that without overwhelming any single site or any single machine. That decision layer is the frontier, and meguri is a frontier built to scale.

## The shape of the problem

Three forces pull on every crawl scheduler at once.

- **Importance.** Some pages matter more than others. A crawl that fetches in discovery order wastes its budget on junk. meguri orders URLs by an importance signal so the next fetch is the most valuable one available.
- **Politeness.** A site is a shared resource. Fetching too fast is rude at best and an outage at worst. meguri holds at most one in-flight fetch per host and per IP, and spaces fetches by a crawl delay it derives from robots and from how the host responds.
- **Freshness.** A page crawled once is a snapshot, not a subscription. meguri estimates how often each page changes and reschedules it on that cycle, so the crawl tracks the web instead of photographing it once.

These forces conflict. The most important page might belong to a host that just answered a request. meguri resolves the conflict by ordering on importance within the set of URLs that politeness currently allows, then revisiting on freshness.

## The key idea: a URL is two halves

meguri identifies a URL by a 128-bit key. The high 64 bits are the host, the low 64 bits are the path. That single choice does a lot of work. Because the host is the high half, all of a host's URLs sort together: they land in the same partition, share one politeness bucket, and sit in a contiguous range of the file. Routing a URL to the machine that owns it, rate-limiting its host, and reading its neighbors off disk all become the same lookup.

## A partition is a file

The frontier is too large for one machine, so it is split into partitions, each owning a range of hosts. A partition is at once a running engine and a file. The engine holds the live queues and heaps; the file, a `.meguri` container, holds the durable state the engine checkpoints to and recovers from. To rebalance the fleet you move files: split a hot partition into two, merge two cold ones into one. The file format is therefore not a serialization afterthought, it is the unit of distribution, and it is documented in full on the [file format](/reference/file-format/) page.

## The crawl loop

A partition does its work in a staged loop. It admits discoveries through the seen-set, orders the admitted URLs by importance, releases only the ones politeness currently allows, dispatches them to the fetcher, folds each outcome back in, and reschedules the URL on its freshness cycle. The fetcher is bound through a small interface, so the same loop runs against an offline corpus in a test and against [ami](https://github.com/tamnd/ami) in production with no change to the scheduler.

You drive that loop from three commands. `meguri seed` builds a frontier from a URL list and checkpoints it; `meguri run` drains a checkpoint in priority-then-politeness order and writes the result back; `meguri serve` opens a directory as a durable, log-structured partition that recovers on restart and checkpoints on shutdown. The [quick start](/getting-started/quick-start/) walks the first two end to end.

## At fleet scale

One partition is a few tens of millions of URLs. A hundred-billion-URL frontier is thousands of them, each a `.meguri` file, routed by host-key range through a partition map. `meguri map` prints that map and routes a single host through it; `meguri pack` and `meguri compact` are the file-side of rebalancing, writing and consolidating partitions; `meguri bench` measures the real per-partition cost on a corpus slice and projects it to the full fleet against the named scaling walls. The [guides](/guides/) cover each of these jobs.

Next: [install meguri](/getting-started/installation/).
