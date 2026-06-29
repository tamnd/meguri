package prioritize

import (
	"testing"

	"github.com/tamnd/meguri"
)

// BenchmarkOPICCredit measures the per-discovery cash credit, the hottest write
// in the prioritizer: every routed out-link pays it. The target is a handful of
// nanoseconds and zero allocations once the URL is resident.
func BenchmarkOPICCredit(b *testing.B) {
	o := NewOPIC(DefaultParams())
	k := key(1, 1)
	o.Seed(k, 0)
	b.ReportAllocs()
	for b.Loop() {
		o.Credit(k, 0.25)
	}
}

// BenchmarkOPICDistribute measures one crawl's cash spread across a typical page's
// out-links, the per-fetch OPIC cost. Links are reused so the bench isolates the
// distribution arithmetic from slice allocation.
func BenchmarkOPICDistribute(b *testing.B) {
	o := NewOPIC(DefaultParams())
	src := key(1, 1)
	const fanout = 32
	links := make([]meguri.Discovery, fanout)
	for i := range links {
		links[i] = meguri.Discovery{URLKey: key(uint64(2+i), 1), SrcHostKey: 1}
	}
	b.ReportAllocs()
	for b.Loop() {
		o.Seed(src, 1) // refresh the cash the prior iteration spent
		o.Distribute(src, links)
	}
}

// BenchmarkOPICScore measures the read every front-bank decision pays. It must be
// allocation-free.
func BenchmarkOPICScore(b *testing.B) {
	o := NewOPIC(DefaultParams())
	k := key(1, 1)
	o.Seed(k, 1)
	b.ReportAllocs()
	var sink float32
	for b.Loop() {
		sink = o.Score(k)
	}
	_ = sink
}

// BenchmarkPriority measures the full blended priority the frontier reads on every
// re-price: OPIC score, import join, host compress, trap and depth penalties. This
// is the number that gates re-bucketing throughput, so it must be allocation-free.
func BenchmarkPriority(b *testing.B) {
	pr := New(DefaultParams())
	rec := &meguri.URLRecord{URLKey: key(1, 1), Depth: 3}
	pr.opic.Seed(rec.URLKey, 1)
	pr.ImportPageRank(rec.URLKey, 0.7)
	h := &meguri.HostRecord{HostScore: 2.0}
	b.ReportAllocs()
	var sink float32
	for b.Loop() {
		sink = pr.Priority(rec, h)
	}
	_ = sink
}
