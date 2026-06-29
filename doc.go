// Package meguri is a distributed web-crawler frontier and rescheduler.
//
// meguri (巡, to make the rounds and revisit on a cycle) is the decision layer
// of a crawl stack: it turns an endless stream of discovered links into a
// politely ordered, freshness-aware crawl schedule for a frontier that scales to
// a hundred billion URLs across a fleet of shared-nothing partitions.
//
// This top-level package holds the data model that every other package shares
// and that the .meguri file serializes straight: the URLKey and HostKey, the
// per-URL and per-host durable records, and the discovery and outcome messages.
// The container itself lives in package format, the in-partition frontier in
// package frontier, and so on per the package layout in the spec.
//
// The design of record is Spec 2071. The pinned canon: the file extension is
// .meguri, the magic is MEG1, the URLKey is 128 bits (HostKey high, PathKey
// low), a partition owns a HostKey range and serializes to one .meguri file,
// and the build is pure Go with CGO disabled.
package meguri
