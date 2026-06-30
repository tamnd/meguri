---
title: "Rebalancing the files"
description: "Route hosts with meguri map, snapshot with pack, and consolidate or split partitions with compact, the file side of fleet rebalancing."
weight: 40
---

A frontier of a hundred billion URLs is thousands of partitions, each a `.meguri` file owning a range of host keys. When one runs hot or two run cold, you rebalance by moving and reshaping files, not by reshuffling rows across a live database. Three commands do the file-side work: `map` routes, `pack` snapshots, and `compact` consolidates.

## Routing a host through the map

A fleet manifest catalogs every partition's host-key range. `meguri map` prints that map and checks the ranges tile the key space with no gap or overlap:

```bash
meguri map --manifest meguri.manifest
```

To find which partition owns a single host, route its key through the map:

```bash
meguri map --manifest meguri.manifest --host 0x3fffffffffffffff
```

The host key is the high 64 bits of a URL key, so routing a host is the same lookup as routing all of its URLs: they always travel together. The manifest is the offline form of the live control plane the fleet routes against.

## Snapshotting a live partition

`meguri pack` writes a partition directory's current live state to a fresh, standalone `.meguri` file:

```bash
meguri pack --data ./part-7 --out part-7.meguri
```

The directory is opened read-only and dropped without a checkpoint, so packing never mutates the running partition. The output is an ordinary `.meguri` any `meguri inspect` opens anywhere, which is what you ship when a partition moves to another machine.

## Consolidating and splitting

`meguri compact` merges one or more `.meguri` files into one partition, re-runs the columnar cascade so the result packs to tens of bytes per URL, and optionally garbage-collects:

```bash
# Merge two cold neighbours into one file.
meguri compact part-7.meguri part-8.meguri --out merged.meguri

# Merge and reclaim space: drop Gone tombstones past their re-probe horizon
# and compact the string arena.
meguri compact part-7.meguri part-8.meguri --out merged.meguri --gc
```

The inputs must own disjoint, ordered host-key ranges. An overlap is reported rather than producing a file a reader would reject, because two partitions claiming the same host would break routing. Merging two cold partitions into one is consolidation; the reverse, splitting a hot partition across a finer range, is the same file machinery run the other way.

## The shape of a rebalance

Put together, a rebalance is: read the map to see which partitions are hot or cold, `pack` a live one to a movable file, `compact` neighbours to merge or split them along a new range, and publish the new manifest so routing follows the files. Because encoding is deterministic and ranges are checked to tile cleanly, every step is verifiable: the same partition always produces the same bytes, and a map that does not tile is caught before it ships.

Next: [project the cost to fleet scale](/guides/projecting-to-scale/).
