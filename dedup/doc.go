// Package dedup is the seen-set: the membership structure that answers "have we
// already discovered this URLKey" so the same URL found a thousand times becomes
// one frontier entry. It layers a fast in-memory probabilistic filter (a blocked
// Bloom or a ribbon filter, around ten bits per URLKey) over an exact on-disk
// set in the DRUM style, so a probable hit is confirmed against the truth and a
// false positive never silently drops a real URL. The 128-bit key derivation and
// URL canonicalization that feed it live here too.
//
// This is the M2 milestone. The package is a placeholder until then.
package dedup
