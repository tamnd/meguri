package freshness

import (
	"math"
	"os"
	"sort"
	"testing"
)

// This file is the offline-replay arm of the M4 freshness benchmark (doc 06,
// doc 14, line 276). The closed-form gate in corpus_test.go scores the policies
// by the analytic Poisson freshness integral; this gate instead replays a real
// change process. For every URL it draws one ground-truth change history (a
// Poisson stream over a fixed horizon, deterministic per URL so there is no
// wall-clock or shared random state), then overlays each scheduling policy's
// crawl grid on that one history and measures what the crawler would actually
// have observed: the fraction of time a page sat stale, the mean age of the
// unobserved change, and the importance-weighted embarrassment.
//
// Measuring all policies against the SAME per-URL history makes it a paired
// comparison: the only thing that differs between policies is where the crawls
// land, not the luck of the change draw. The four policies are the doc 06
// section 10 set:
//
//   - uniform: every URL crawled at the same rate (budget / n).
//   - proportional: crawl rate proportional to change rate, the "chase what
//     changes most" rule the hump refutes.
//   - threshold-optimal: the water-filling allocation at the exact dual price
//     that meets the budget, the offline optimum of the freshness objective.
//   - meguri: the same water-filling rule, but with the dual price rediscovered
//     by the TauController feedback loop from a cold start, the incremental
//     approximation the partition actually runs (doc 06, section 8).
//
// meguri must beat both naive baselines and land within a small margin of the
// offline optimum, which is the convergence claim the incremental controller
// rests on. The change rates are the real per-URL estimates where the corpus
// captured a URL more than once and the same deterministic heavy-tailed draw as
// the closed-form gate otherwise (D19, the modelled tail); the ground-truth
// per-URL change history sampled from a real change-rate sample is the doc 14
// follow-up that lands with the ami capture sample.

// horizonHours is the replay window, two weeks of epoch-hours. It is long
// enough that even a slowly-funded page is crawled several times, so the
// time-average is meaningful, and short enough that the hottest pages do not
// generate an unbounded event stream.
const horizonHours = 14 * 24

// policyResult holds the three metrics one policy reaches over the replay.
type policyResult struct {
	embarrassment float64 // importance-weighted mean staleness fraction, the doc 06 section 1 objective
	staleness     float64 // unweighted mean staleness fraction
	age           float64 // unweighted mean age of the unobserved change, hours
}

// generateEvents draws one URL's ground-truth change history: a Poisson process
// of rate lambda over [0, horizon], returned as sorted event times in hours. The
// stream is seeded only by the URL, so the history is reproducible and carries no
// wall-clock or global random state. A hard cap bounds the hottest pages so the
// replay stays quick; a page that would change more than the cap allows is
// already hopeless to keep fresh and its extra events do not move the staleness.
func generateEvents(seed uint64, lambda, horizon float64) []float64 {
	const maxEvents = 1 << 14
	st := seed
	var events []float64
	t := 0.0
	for len(events) < maxEvents {
		st = splitmix(st)
		u := float64(st>>11) / float64(1<<53)
		if u <= 0 {
			u = 1e-18
		}
		t += -math.Log(u) / lambda
		if t > horizon {
			break
		}
		events = append(events, t)
	}
	return events
}

// replayPolicy overlays a regular crawl grid of one interval on a URL's
// ground-truth change history and returns the time-averaged staleness fraction
// and mean age in hours. Each crawl interval ends with a crawl that observes and
// resets the page, so staleness never crosses an interval boundary: within an
// interval only the first change matters, and the page sits stale from that
// change to the crawl at the interval end. A URL whose interval is longer than
// the horizon is modelled as crawled once at the horizon end.
func replayPolicy(events []float64, rate, horizon float64) (staleFrac, meanAge float64) {
	interval := 1.0 / rate
	if interval > horizon {
		interval = horizon
	}
	var stale, ageIntegral float64
	lastIdx := -1
	for _, tc := range events {
		idx := int(tc / interval)
		if idx == lastIdx {
			continue // a later change in an already-stale interval adds nothing
		}
		lastIdx = idx
		end := float64(idx+1) * interval
		if end > horizon {
			end = horizon
		}
		d := end - tc
		if d <= 0 {
			continue
		}
		stale += d
		ageIntegral += 0.5 * d * d // age ramps from 0 to d over the stale span
	}
	return stale / horizon, ageIntegral / horizon
}

// replay scores one rate vector over the whole population against the shared
// ground-truth histories, returning the weighted embarrassment and the unweighted
// staleness and age means.
func replay(events [][]float64, rates, weights []float64) policyResult {
	var embNum, embDen, staleSum, ageSum float64
	for i := range rates {
		s, a := replayPolicy(events[i], rates[i], horizonHours)
		embNum += weights[i] * s
		embDen += weights[i]
		staleSum += s
		ageSum += a
	}
	n := float64(len(rates))
	return policyResult{
		embarrassment: embNum / embDen,
		staleness:     staleSum / n,
		age:           ageSum / n,
	}
}

// TestCorpusReplayBeatsBaselines is the offline-replay gate (doc 06, doc 14).
// It builds the real population, draws one ground-truth change history per URL,
// allocates crawl rates under four policies sharing one budget, replays each
// against the histories, and requires the water-filling policies to beat the two
// naive baselines on weighted embarrassment with meguri converging to within a
// small margin of the offline optimum.
func TestCorpusReplayBeatsBaselines(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	p := DefaultParams()
	all := loadCorpus(t, path)

	// Population: real lambda from observed captures where present, the modelled
	// heavy-tailed draw otherwise. Cap the single-capture sample so the replay
	// stays quick while keeping every real repeated-capture URL.
	const singleCap = 6000
	var lambdas, weights []float64
	var realN, modelN int
	for _, cu := range all {
		var lambda float64
		if len(cu.caps) >= 2 {
			rec := recordFromHistory(cu)
			if rec.LastCrawled > rec.FirstSeen {
				lambda = Estimate(rec, p)
				realN++
			}
		}
		if lambda == 0 {
			if modelN >= singleCap {
				continue
			}
			lambda = modelLambda(cu.url, p)
			modelN++
		}
		lambdas = append(lambdas, lambda)
		weights = append(weights, cu.priority)
	}
	n := len(lambdas)
	if n < 1000 {
		t.Fatalf("population too small: %d URLs", n)
	}

	// Ground-truth histories, one Poisson draw per URL, seeded by index so the
	// draw is reproducible and independent of the URL strings the policies share.
	events := make([][]float64, n)
	var totalEvents int
	for i := range lambdas {
		events[i] = generateEvents(splitmix(uint64(i)+1), lambdas[i], horizonHours)
		totalEvents += len(events[i])
	}
	t.Logf("replay gate: %d URLs (%d real-change, %d modelled-tail), %d ground-truth change events over %dh",
		n, realN, modelN, totalEvents, horizonHours)

	// threshold-optimal: the water-filling allocation at the dual price that funds
	// the top ~40 percent of pages, a genuinely budget-constrained regime. Its
	// total spend is the shared budget every policy is held to.
	vds := make([]float64, n)
	for i := range lambdas {
		vds[i] = ValueDensity(urlAt(lambdas[i], weights[i]), p)
	}
	sorted := append([]float64(nil), vds...)
	sort.Float64s(sorted)
	tauOpt := sorted[n*60/100]

	optimal := make([]float64, n)
	var budget float64
	for i := range lambdas {
		rate := 1.0 / TargetInterval(urlAt(lambdas[i], weights[i]), nil, tauOpt, p)
		optimal[i] = rate
		budget += rate
	}

	// uniform and proportional spend the same budget, allocated by the two naive
	// rules.
	uniform := make([]float64, n)
	u := budget / float64(n)
	var sumLambda float64
	for _, l := range lambdas {
		sumLambda += l
	}
	proportional := make([]float64, n)
	for i := range lambdas {
		uniform[i] = u
		proportional[i] = budget * lambdas[i] / sumLambda
	}

	// meguri: the same water-filling rule, but with tau rediscovered by the
	// controller from a cold start. Tick it on the population's total scheduled
	// rate until it settles, then read the rates off the settled price.
	ctrl := NewTauController(budget)
	for range 400 {
		var scheduled float64
		for i := range lambdas {
			scheduled += 1.0 / TargetInterval(urlAt(lambdas[i], weights[i]), nil, ctrl.Tau(), p)
		}
		ctrl.Tick(scheduled)
	}
	meguri := make([]float64, n)
	for i := range lambdas {
		meguri[i] = 1.0 / TargetInterval(urlAt(lambdas[i], weights[i]), nil, ctrl.Tau(), p)
	}

	rOpt := replay(events, optimal, weights)
	rUni := replay(events, uniform, weights)
	rPro := replay(events, proportional, weights)
	rMeg := replay(events, meguri, weights)

	t.Logf("replay gate: embarrassment optimal=%.5f meguri=%.5f uniform=%.5f proportional=%.5f (budget=%.1f crawls/h)",
		rOpt.embarrassment, rMeg.embarrassment, rUni.embarrassment, rPro.embarrassment, budget)
	t.Logf("replay gate: staleness  optimal=%.5f meguri=%.5f uniform=%.5f proportional=%.5f", rOpt.staleness, rMeg.staleness, rUni.staleness, rPro.staleness)
	t.Logf("replay gate: mean age/h optimal=%.3f meguri=%.3f uniform=%.3f proportional=%.3f", rOpt.age, rMeg.age, rUni.age, rPro.age)

	// The objective the rescheduler minimizes is the importance-weighted
	// embarrassment, not the unweighted staleness or age: those two columns are
	// reported only to expose the hump's deliberate trade-off. The optimum funds
	// the medium-rate important pages and starves both the near-static pages
	// (already fresh) and the hyper-volatile ones (hopeless), so it carries a
	// higher unweighted staleness and age than uniform even as it wins the weighted
	// objective. That uniform beats proportional on every column is the
	// Cho-Garcia-Molina counterintuitive-freshness result, reproduced here on the
	// real change process. The assertions below therefore gate only on
	// embarrassment.

	// The offline optimum beats both naive baselines on the real change process,
	// not just on the closed form.
	if !(rOpt.embarrassment < rUni.embarrassment) {
		t.Errorf("threshold-optimal did not beat uniform: optimal=%.5f uniform=%.5f", rOpt.embarrassment, rUni.embarrassment)
	}
	if !(rOpt.embarrassment < rPro.embarrassment) {
		t.Errorf("threshold-optimal did not beat proportional: optimal=%.5f proportional=%.5f", rOpt.embarrassment, rPro.embarrassment)
	}
	// meguri beats both baselines too.
	if !(rMeg.embarrassment < rUni.embarrassment) {
		t.Errorf("meguri did not beat uniform: meguri=%.5f uniform=%.5f", rMeg.embarrassment, rUni.embarrassment)
	}
	if !(rMeg.embarrassment < rPro.embarrassment) {
		t.Errorf("meguri did not beat proportional: meguri=%.5f proportional=%.5f", rMeg.embarrassment, rPro.embarrassment)
	}
	// The incremental controller converges to within a small margin of the offline
	// optimum: the only gap is the controller's dead band and cold-start settling,
	// not a different allocation rule (doc 06, section 8).
	if rMeg.embarrassment > rOpt.embarrassment*1.10 {
		t.Errorf("meguri did not converge to the optimum: meguri=%.5f optimum=%.5f (>10%% gap)", rMeg.embarrassment, rOpt.embarrassment)
	}
}

// TestReplayMatchesClosedForm validates the simulator against the analytic
// answer it is meant to reproduce: for a single fixed change rate crawled at a
// fixed interval, the replayed time-averaged staleness must match the Poisson
// freshness complement 1 - (1 - e^{-lambda*I}) / (lambda*I). It runs without a
// corpus so the simulator itself is always checked, and it pins the meaning of
// the staleness number the corpus gate reports.
func TestReplayMatchesClosedForm(t *testing.T) {
	const lambda = 0.05 // a change roughly every twenty hours
	const interval = 12.0
	const horizon = 200000.0 // a long horizon to average out the Monte-Carlo noise

	events := generateEvents(splitmix(42), lambda, horizon)
	gotStale, _ := replayPolicy(events, 1.0/interval, horizon)

	x := lambda * interval
	wantStale := 1 - (1-math.Exp(-x))/x
	if math.Abs(gotStale-wantStale) > 0.01 {
		t.Fatalf("replayed staleness %.5f, closed form %.5f, gap %.5f exceeds tolerance", gotStale, wantStale, math.Abs(gotStale-wantStale))
	}
	t.Logf("replay matches closed form: simulated staleness %.5f vs analytic %.5f", gotStale, wantStale)
}
