// Package dedup is the seen-set and the content-dedup machinery: the layer that
// keeps the same URL discovered a thousand times to one frontier entry, and the
// same content served under a thousand URLs to one crawl (doc 08, D5, D7, D17).
//
// The seen-set is two tiers. A resident approximate filter (a one-sided blocked
// Bloom filter, around eleven bits per URL at the 1% default) answers "definitely
// not seen" authoritatively; a "probably seen" hit is confirmed against an exact
// on-disk key set consulted through DRUM batching, so a false positive costs a
// batched sequential confirm, never a dropped page, and a false negative never
// happens. SeenSet wraps the two and exposes the idempotent check-and-insert that
// makes discovery tolerate at-least-once delivery.
//
// Canonicalization (the eleven steps of doc 03), the registrable-domain grouping
// over the embedded Public Suffix List, and the 128-bit URLKey derivation feed
// the seen-set: a URL is canonicalized once at discovery, before it is keyed.
//
// Content dedup is a 64-bit exact content fingerprint and a 64-bit Charikar
// simhash with Hamming threshold 3, computed at fetch time, used to tell a real
// edit from a rotating ad and to collapse the same content under different URLs.
// The trap defense is the per-host budget and depth cap, soft-404 detection by
// content fingerprint, and near-dup recovery of the budget a pure cap would waste.
//
// This is the M2 milestone.
package dedup
