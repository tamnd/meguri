---
title: "CLI reference"
description: "Every command and flag the meguri binary exposes."
weight: 20
---

The `meguri` binary is the front door to the frontier engine and its files. In M0 it carries the file tools; the crawl-loop commands arrive with the engine.

## meguri

```
meguri [command]
```

Run with no command for the help screen. Global flags:

- `--version` prints the version, commit, and build date.
- `-h`, `--help` prints help for the binary or any subcommand.

## meguri inspect

```
meguri inspect <file.meguri>
```

Print the structure and stats of a `.meguri` file: the header facts, the region layout, the column counts, the checksum and codec, and the at-a-glance stats. The summary is computed from the header and the footer, so the cost is two small reads regardless of file size.

```bash
meguri inspect partition-00007.meguri
```

The output is documented field by field on the [quick start](/getting-started/quick-start/) page.
