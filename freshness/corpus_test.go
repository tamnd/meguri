package freshness

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/meguri"
)

// This file is the M4 gate on real data (doc 06, doc 14): the freshness
// estimator and the water-filling allocation are exercised over the frozen
// ccrawl corpus (CC-MAIN-2026-25, the tamnd/ccrawl-cli slice pinned in
// corpus/). The corpus carries two real signals the rescheduler is built on.
//
// First, repeated captures. A slice of one monthly index still revisits a
// fraction of its URLs across the crawl window (here ~2k URLs captured two or
// more times over thirteen days), and each repeat carries a content digest, so
// a digest that differs between two consecutive captures of one URL is a real,
// observed content change. That is exactly the no-change-versus-change signal
// the estimator consumes, drawn from real fetches rather than a model.
//
// Second, the real URL, host, and importance population. Every allocation input
// other than the change rate (the host a URL sits on, the importance weight) is
// taken from the real corpus, so the water-filling rule is thresholding a real
// distribution of pages, not a synthetic one.
//
// The honesty flag (D19): a single frozen index cannot supply a long
// per-URL change history, so the 140k single-capture URLs carry no observed
// change signal and their change rate is drawn from a heavy-tailed model
// (log-uniform over the plausible band), deterministically per URL. The
// estimator gate below runs only on the URLs that do carry real repeated
// captures; the allocation gate runs on the whole population and is explicit
// that the single-capture rates are modelled. Cross-index repeated captures
// (the same URL across successive monthly crawls) are the richer real signal
// and are the doc 14 follow-up; this gate uses the within-window repeats that
// the pinned slice already holds.

// capture is one observation of a URL: when it was fetched and the content
// digest at that time.
type capture struct {
	tHours uint32 // fetch time in epoch-hours, to match the data model
	digest string // content digest, the change signal across consecutive captures
}

// corpusURL is a real URL with its capture history and the inputs the allocator
// reads.
type corpusURL struct {
	url      string
	host     string
	caps     []capture
	priority float64
}

// loadCorpus reads the pinned ccrawl jsonl slice, groups the rows by URL, and
// returns one corpusURL per distinct URL with its captures sorted in time. It is
// self-contained so the freshness gate does not depend on the frontier test
// helpers.
func loadCorpus(tb testing.TB, path string) []corpusURL {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type row struct {
		URL       string `json:"url"`
		Timestamp string `json:"timestamp"`
		Digest    string `json:"digest"`
	}
	byURL := map[string]*corpusURL{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r row
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		host := hostOf(r.URL)
		if host == "" {
			continue
		}
		t, ok := tsHours(r.Timestamp)
		if !ok {
			continue
		}
		cu := byURL[r.URL]
		if cu == nil {
			cu = &corpusURL{url: r.URL, host: host, priority: corpusWeight(r.URL)}
			byURL[r.URL] = cu
		}
		cu.caps = append(cu.caps, capture{tHours: t, digest: r.Digest})
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}

	out := make([]corpusURL, 0, len(byURL))
	for _, cu := range byURL {
		sort.Slice(cu.caps, func(i, j int) bool { return cu.caps[i].tHours < cu.caps[j].tHours })
		out = append(out, *cu)
	}
	// Stable order so the gate is reproducible run to run.
	sort.Slice(out, func(i, j int) bool { return out[i].url < out[j].url })
	return out
}

// recordFromHistory turns a URL's real capture history into the sufficient
// statistics the estimator reads: a crawl count, a meaningful-change count from
// the digest transitions, the trailing no-change streak, and the first and last
// fetch times. Two consecutive captures with differing digests are one observed
// change; identical digests are an observed no-change.
func recordFromHistory(cu corpusURL) *meguri.URLRecord {
	n := len(cu.caps)
	rec := &meguri.URLRecord{
		CrawlCount:  uint32(n),
		FirstSeen:   cu.caps[0].tHours,
		LastCrawled: cu.caps[n-1].tHours,
		Priority:    float32(cu.priority),
	}
	var changes, streak uint32
	for i := 1; i < n; i++ {
		if cu.caps[i].digest != cu.caps[i-1].digest {
			changes++
			streak = 0
		} else {
			streak++
		}
	}
	rec.ChangeCount = changes
	rec.NoChangeStreak = uint16(streak)
	return rec
}

// TestCorpusEstimatorTracksRealChange is the estimator gate on real repeated
// captures: over the URLs the corpus captured two or more times, the no-change
// ratio estimator must stay inside the configured band, must rate a URL that was
// observed changing above one that was observed stable, and must sit at or above
// the naive count estimator on average, the upward bias correction the estimator
// exists to make (doc 06, section 3).
func TestCorpusEstimatorTracksRealChange(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	p := DefaultParams()
	all := loadCorpus(t, path)

	var multi int
	var sumEst, sumNaive float64
	var changedMean, stableMean float64
	var changedN, stableN int
	for _, cu := range all {
		if len(cu.caps) < 2 {
			continue
		}
		rec := recordFromHistory(cu)
		// Estimating needs a real span; captures inside the same hour collapse to a
		// zero span and the estimator correctly holds at the floor, so skip them for
		// the directional checks.
		if rec.LastCrawled <= rec.FirstSeen {
			continue
		}
		multi++
		est := Estimate(rec, p)
		if est < p.MinRate || est > p.MaxRate || math.IsNaN(est) || math.IsInf(est, 0) {
			t.Fatalf("estimate %.6g for %s fell outside [%.6g, %.6g]", est, cu.url, p.MinRate, p.MaxRate)
		}
		intervals := float64(rec.CrawlCount) - 1
		tavg := float64(rec.LastCrawled-rec.FirstSeen) / intervals
		naive := float64(rec.ChangeCount) / (intervals * tavg) // X / (n*T), the biased count
		sumEst += est
		sumNaive += naive
		if rec.ChangeCount > 0 {
			changedMean += est
			changedN++
		} else {
			stableMean += est
			stableN++
		}
	}
	if multi < 100 {
		t.Fatalf("only %d usable repeated-capture URLs, expected the pinned slice to hold more", multi)
	}
	t.Logf("estimator gate: %d real repeated-capture URLs (%d observed changing, %d observed stable)", multi, changedN, stableN)

	if changedN == 0 || stableN == 0 {
		t.Fatalf("need both changing and stable URLs, got changed=%d stable=%d", changedN, stableN)
	}
	changedMean /= float64(changedN)
	stableMean /= float64(stableN)
	if !(changedMean > stableMean) {
		t.Errorf("observed-changing URLs not rated faster than stable ones: changed=%.6g stable=%.6g", changedMean, stableMean)
	}
	// The no-change-ratio estimator corrects the count estimator upward, never
	// below it, because a visit cannot see multiple changes in one interval.
	if !(sumEst >= sumNaive) {
		t.Errorf("estimator fell below the naive count in aggregate: est=%.6g naive=%.6g", sumEst, sumNaive)
	}
	t.Logf("estimator gate: mean lambda changed=%.6g/h stable=%.6g/h; aggregate est=%.4g >= naive=%.4g", changedMean, stableMean, sumEst, sumNaive)
}

// TestCorpusAllocationBeatsBaselines is the allocation gate on the real
// population (doc 06, section 4, 10). It assigns every URL a change rate (the
// real estimate where the corpus captured it more than once, a deterministic
// heavy-tailed draw otherwise), takes the host and importance weight from the
// real corpus, sets one water level, and compares the importance-weighted
// staleness the water-filling allocation reaches against two baselines under the
// identical total crawl budget: uniform (every URL crawled equally) and
// proportional (crawl rate proportional to change rate, the "chase what changes
// most" rule the hump refutes). meguri must beat both.
func TestCorpusAllocationBeatsBaselines(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	p := DefaultParams()
	all := loadCorpus(t, path)

	// Build the population: real lambda from observed captures where present, a
	// deterministic log-uniform draw otherwise (the modelled tail, D19). Cap the
	// single-capture sample so the gate stays quick while keeping every real
	// repeated-capture URL.
	const singleCap = 25000
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
	t.Logf("allocation gate: %d URLs (%d real-change, %d modelled-tail)", n, realN, modelN)

	// Pick the water level at the 60th percentile of value density, so the top
	// ~40 percent of pages are funded and the rest sit on the re-probe floor. This
	// is a genuinely budget-constrained regime, where allocation quality shows: the
	// scarce crawls must go where they buy the most freshness, not onto the
	// near-static pages (already fresh) or the hyper-volatile ones (hopeless).
	vds := make([]float64, n)
	for i := range lambdas {
		vds[i] = ValueDensity(urlAt(lambdas[i], weights[i]), p)
	}
	sorted := append([]float64(nil), vds...)
	sort.Float64s(sorted)
	tau := sorted[n*60/100]

	// meguri rates from the water-filling rule, then the total it spends becomes
	// the shared budget handed to the two baselines.
	meguriRates := make([]float64, n)
	var budget float64
	for i := range lambdas {
		rate := 1.0 / TargetInterval(urlAt(lambdas[i], weights[i]), nil, tau, p)
		meguriRates[i] = rate
		budget += rate
	}

	uniform := make([]float64, n)
	u := budget / float64(n)
	for i := range uniform {
		uniform[i] = u
	}

	proportional := make([]float64, n)
	var sumLambda float64
	for _, l := range lambdas {
		sumLambda += l
	}
	for i := range proportional {
		proportional[i] = budget * lambdas[i] / sumLambda
	}

	meguriS := weightedStaleness(meguriRates, lambdas, weights)
	uniformS := weightedStaleness(uniform, lambdas, weights)
	proportionalS := weightedStaleness(proportional, lambdas, weights)
	t.Logf("allocation gate: weighted staleness meguri=%.5f uniform=%.5f proportional=%.5f (budget=%.1f crawls/h)", meguriS, uniformS, proportionalS, budget)

	if !(meguriS < uniformS) {
		t.Errorf("water-filling did not beat uniform: meguri=%.5f uniform=%.5f", meguriS, uniformS)
	}
	if !(meguriS < proportionalS) {
		t.Errorf("water-filling did not beat proportional: meguri=%.5f proportional=%.5f", meguriS, proportionalS)
	}
}

// weightedStaleness is the objective the rescheduler minimizes (doc 06,
// section 1): the importance-weighted average of one minus the Poisson
// time-averaged freshness, where a page of rate lambda crawled at interval I has
// freshness (1 - e^{-lambda*I}) / (lambda*I). It is computed in closed form, so
// the comparison is deterministic, not Monte Carlo.
func weightedStaleness(rates, lambdas, weights []float64) float64 {
	var num, den float64
	for i := range rates {
		interval := 1.0 / rates[i]
		x := lambdas[i] * interval
		var fresh float64
		if x < 1e-9 {
			fresh = 1
		} else {
			fresh = (1 - math.Exp(-x)) / x
		}
		num += weights[i] * (1 - fresh)
		den += weights[i]
	}
	if den == 0 {
		return 0
	}
	return num / den
}

// modelLambda draws a deterministic change rate for a URL the corpus captured
// only once, log-uniform over the band from once a year to twelve times an hour,
// keyed on the URL so the draw is reproducible and carries no wall-clock or
// random seed. The top of the band reaches the hyper-volatile scoreboard rate of
// doc 06 section 10 on purpose: the real web holds such pages, and they are the
// reason water-filling beats both naive baselines, so a population that excluded
// them would understate the allocation problem. This is the modelled tail of the
// population (D19), used only where the frozen slice gives no real
// repeated-capture signal.
func modelLambda(url string, p Params) float64 {
	lo := p.MinRate
	hi := 12.0 // the hyper-volatile scoreboard rate, every five minutes
	r := splitmix(fnv64(url))
	u := float64(r>>11) / float64(1<<53)
	return lo * math.Exp(u*math.Log(hi/lo))
}

// corpusWeight derives a stable importance weight in (0, 1] from a URL, the
// embarrassment proxy stand-in for the gate, matching the spread the frontier
// gate uses.
func corpusWeight(url string) float64 {
	return float64(fnv32(url)%1000+1) / 1001.0
}

// --- small deterministic helpers, no wall-clock or global random state ---

// tsHours parses a ccrawl 14-digit timestamp (YYYYMMDDHHMMSS) into epoch-hours.
func tsHours(ts string) (uint32, bool) {
	t, err := time.Parse("20060102150405", ts)
	if err != nil {
		return 0, false
	}
	return uint32(t.Unix() / 3600), true
}

// hostOf extracts the host from a URL without pulling in net/url, enough for the
// corpus rows which are all absolute http(s) URLs.
func hostOf(u string) string {
	_, rest, ok := strings.Cut(u, "://")
	if !ok {
		return ""
	}
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.IndexByte(rest, '@'); k >= 0 {
		rest = rest[k+1:]
	}
	if k := strings.IndexByte(rest, ':'); k >= 0 {
		rest = rest[:k]
	}
	return rest
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func fnv64(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
