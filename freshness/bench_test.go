package freshness

import (
	"math"
	"os"
	"testing"

	"github.com/tamnd/meguri"
)

// benchPopulation builds the records the freshness benchmarks run over. With
// MEGURI_CORPUS set it draws the real population from the pinned ccrawl slice
// (real change history where the corpus repeats a capture, a modelled rate
// otherwise); without it, a deterministic synthetic spread so the benchmark
// still runs in CI. Both feed the same per-URL functions the hot path calls.
func benchPopulation(tb testing.TB) []*meguri.URLRecord {
	p := DefaultParams()
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		out := make([]*meguri.URLRecord, 20000)
		for i := range out {
			r := splitmix(uint64(i))
			u := float64(r>>11) / float64(1<<53)
			lambda := p.MinRate * math.Pow(12.0/p.MinRate, u)
			out[i] = &meguri.URLRecord{
				Lambda:      float32(lambda),
				Priority:    float32((r%1000 + 1)) / 1001.0,
				CrawlCount:  uint32(2 + r%30),
				ChangeCount: uint32(r % 10),
				FirstSeen:   0,
				LastCrawled: uint32(24 * (1 + r%30)),
			}
		}
		return out
	}
	all := loadCorpus(tb, path)
	out := make([]*meguri.URLRecord, 0, len(all))
	for _, cu := range all {
		var rec *meguri.URLRecord
		if len(cu.caps) >= 2 {
			rec = recordFromHistory(cu)
		} else {
			rec = &meguri.URLRecord{
				Lambda:      float32(modelLambda(cu.url, p)),
				Priority:    float32(cu.priority),
				CrawlCount:  1,
				FirstSeen:   cu.caps[0].tHours,
				LastCrawled: cu.caps[0].tHours,
			}
		}
		out = append(out, rec)
	}
	return out
}

// BenchmarkEstimate measures the change-rate estimator on the real population,
// the per-URL cost the rescheduler pays on every crawl outcome.
func BenchmarkEstimate(b *testing.B) {
	p := DefaultParams()
	pop := benchPopulation(b)
	b.ReportAllocs()
	var sink float64
	i := 0
	for b.Loop() {
		sink += Estimate(pop[i%len(pop)], p)
		i++
	}
	_ = sink
}

// BenchmarkTargetInterval measures the water-filling allocation on the real
// population, the second per-URL cost on the reschedule path.
func BenchmarkTargetInterval(b *testing.B) {
	p := DefaultParams()
	pop := benchPopulation(b)
	b.ReportAllocs()
	var sink float64
	i := 0
	for b.Loop() {
		sink += TargetInterval(pop[i%len(pop)], nil, 1e-3, p)
		i++
	}
	_ = sink
}
