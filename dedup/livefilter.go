package dedup

import "github.com/tamnd/meguri"

// Filter is the resident approximate seen-set tier on its own, the doc 04 blocked
// Bloom without the exact set behind it. The file-backed engine (spec 2073 doc 08)
// uses it as the negative-fast dedup tier in front of the mapped .meguri file: a
// miss is an authoritative "this URL is new" that never touches the file, and the
// file's sorted keys are the exact set a positive is confirmed against. SeenSet
// pairs the same filter with a resident exact set, which is O(distinct URLs) and is
// the term doc 08 moves onto disk; Filter is the filter alone, for a caller whose
// exact set is the mapped file.
type Filter struct{ f *filter }

// NewFilter builds an empty resident filter sized for capacity keys at the given
// false-positive rate. The rate is a throughput knob, not a correctness one: a
// higher rate means more confirmations against the file, never a dropped key,
// because the filter is one-sided and has no false negatives.
func NewFilter(capacity uint64, fpRate float64) *Filter {
	return &Filter{f: newFilter(capacity, fpRate)}
}

// Add records a key as seen.
func (r *Filter) Add(key meguri.URLKey) { r.f.add(key) }

// MaybeContains reports the filter's verdict. A false is authoritative: the key
// was never added, so it is new and no file lookup is needed. A true is the
// filter's "probably", which on an unseen key is a false positive the caller
// confirms against the exact set on disk.
func (r *Filter) MaybeContains(key meguri.URLKey) bool { return r.f.maybeSeen(key) }

// BitsPerURL reports the resident filter cost per added key, the budget the
// residency gate checks.
func (r *Filter) BitsPerURL() float64 { return r.f.bitsPerKey() }

// Len reports the number of keys added.
func (r *Filter) Len() uint64 { return r.f.length() }

// Marshal serializes the filter for the .meguri seen-set region, so a reload
// restores it without re-adding every key.
func (r *Filter) Marshal() []byte { return r.f.marshal() }

// LoadFilter restores a Filter from Marshal bytes, the recovery path that reads
// the seen-set region of a mapped file straight into the resident tier.
func LoadFilter(b []byte) (*Filter, error) {
	f, err := unmarshalBloom(b)
	if err != nil {
		return nil, err
	}
	return &Filter{f: f}, nil
}
