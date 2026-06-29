// Package format is the .meguri single-file container: the 64-byte header, the
// five regions (URL table, host table, schedule index, seen-set filter, string
// and blob), and the footer written last, bracketed by the magic MEG1 at both
// ends.
//
// A .meguri file is one partition's entire frontier state. It is at once the
// durable checkpoint, the redistribution unit when a partition moves or
// rebalances, and the cold archive (D1, D12). The footer comes last so one read
// of the file tail learns the whole structure, which is what makes object-store
// redistribution cheap.
//
// This package owns the bytes. The logical schema it serializes (URLRecord,
// HostRecord, and the rest) lives in the top-level meguri package, and a
// checkpoint writes those records straight into columns with no remapping. The
// writer is deterministic: given the same records and the same creation
// timestamp it produces byte-identical files, the property every golden-file
// test and content-addressed redistribution leans on.
//
// M0 lands the container with RAW column encoding plus an optional zstd block
// codec. The richer tatami encoding cascade (dictionary, delta, frame of
// reference, FSST) slots in behind the same page and directory structure in
// later milestones without changing the container.
package format
