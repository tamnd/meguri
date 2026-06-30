package freshness

import (
	"encoding/json"
	"math"
	"os"
	"slices"
	"testing"

	"github.com/tamnd/meguri"
)

// TestLambdaConvergence measures how fast the change-rate estimator finds a known
// true Poisson rate as a page's crawl history grows, the recrawler claim spec 2073
// doc 02 has to back with a number rather than a derivation. It is gated behind
// MEGURI_MEASURE so it stays out of normal CI; set MEGURI_MEASURE=1 to capture it.
//
// For each true rate lambda it simulates a population of independent pages crawled
// at the page's own period T = 1/lambda (the rate a converged recrawler would pick,
// section 4). Each inter-crawl gap is a Bernoulli draw on the Poisson no-change
// probability e^{-lambda*T}, folded into the record's counters exactly as the
// frontier's markCrawled does (CrawlCount, ChangeCount, NoChangeStreak, LastCrawled).
// After each crawl it recomputes Estimate and, at the reported crawl counts, records
// the ratio of the estimate to the truth across the population. The estimator works
// off the no-change fraction, which is observed exactly, so the population mean ratio
// converges to one as the history lengthens; the spread shows how noisy a single
// page's estimate is at a given history length, which is what sets how long a page
// must be crawled before its schedule is trustworthy.
func TestLambdaConvergence(t *testing.T) {
	if os.Getenv("MEGURI_MEASURE") == "" {
		t.Skip("set MEGURI_MEASURE=1 to run lambda convergence")
	}
	p := DefaultParams()
	trueRates := []float64{0.002, 0.01, 0.05, 0.2} // changes/hour: ~3 weeks down to ~5 hours
	report := map[int]bool{2: true, 4: true, 8: true, 16: true, 32: true, 64: true, 128: true, 256: true}
	const pop = 4000
	const maxCrawls = 256

	for _, lambda := range trueRates {
		T := max(uint32(math.Round(1.0/lambda)), 1)
		noChangeP := math.Exp(-lambda * float64(T))

		// ratios[c] holds, for every page, the estimate/truth ratio after c crawls.
		ratios := make(map[int][]float64, len(report))
		for c := range report {
			ratios[c] = make([]float64, 0, pop)
		}
		for u := range pop {
			rng := splitmix(uint64(u)*0x9e3779b97f4a7c15 + uint64(T))
			rec := &meguri.URLRecord{FirstSeen: 1, LastCrawled: 1}
			prevFP := uint64(0)
			for c := 1; c <= maxCrawls; c++ {
				// One crawl T hours after the last. Draw change or no-change from the
				// Poisson no-change probability and fold it the way markCrawled does.
				rng = splitmix(rng)
				u01 := float64(rng>>11) / float64(1<<53)
				rec.CrawlCount++
				if c > 1 {
					rec.LastCrawled += T
				}
				if prevFP != 0 {
					if u01 < noChangeP {
						rec.NoChangeStreak++
					} else {
						rec.ChangeCount++
						rec.NoChangeStreak = 0
					}
				}
				prevFP = 1 // every fetch leaves a fingerprint to compare against next time

				if report[c] {
					hat := Estimate(rec, p)
					ratios[c] = append(ratios[c], hat/lambda)
				}
			}
		}

		for c := 1; c <= maxCrawls; c++ {
			rs, ok := ratios[c]
			if !ok || len(rs) == 0 {
				continue
			}
			slices.Sort(rs)
			var sum float64
			for _, r := range rs {
				sum += r
			}
			mean := sum / float64(len(rs))
			med := rs[len(rs)/2]
			p90 := rs[int(0.90*float64(len(rs)))]
			out := map[string]any{
				"true_lambda":  lambda,
				"period_hours": T,
				"crawl_count":  c,
				"n_pages":      len(rs),
				"mean_ratio":   round4(mean),
				"median_ratio": round4(med),
				"p90_ratio":    round4(p90),
				"p10_ratio":    round4(rs[int(0.10*float64(len(rs)))]),
				"mean_rel_err": round4(math.Abs(mean - 1)),
			}
			b, _ := json.Marshal(out)
			t.Logf("LAMBDA_CONVERGENCE %s", b)
		}
	}
}

func round4(v float64) float64 { return math.Round(v*1e4) / 1e4 }
