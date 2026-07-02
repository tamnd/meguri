package frontier

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

// TestDispatchLatencyProfile profiles the scheduler's per-selection dispatch cost
// as a latency distribution, the number spec 2073 doc 02 reports for the engine's
// hot path. It is gated behind MEGURI_MEASURE so it never runs in normal CI, where
// it would only burn time; set MEGURI_MEASURE=1 to capture it on a box of record.
//
// Two passes over a freshly seeded frontier. Pass A drains it untimed and reports
// the clean aggregate rate (selections divided by wall), the sel/s with no per-call
// timer in the loop. Pass B times each Dispatch call and reports the distribution.
// Both seed with crawl delay zero so a host reopens the instant its outcome is
// reported: the loop never sleeps on politeness, so every Dispatch yields a
// selection and the measured cost is the selection machinery alone (promote the due
// and front banks, pop the best ready host, resolve and re-check its window, pick
// the head URL, spend politeness, fold the outcome), not any politeness delay. With
// MEGURI_CORPUS set the seeds are the real ccrawl slice; otherwise a synthetic
// spread sized by MEGURI_MEASURE_HOSTS and MEGURI_MEASURE_PERHOST.
func TestDispatchLatencyProfile(t *testing.T) {
	if os.Getenv("MEGURI_MEASURE") == "" {
		t.Skip("set MEGURI_MEASURE=1 to run the dispatch latency profile")
	}
	var seeds []seed
	if path := os.Getenv("MEGURI_CORPUS"); path != "" {
		seeds = loadCorpusSeeds(t, path)
	} else {
		hosts := envInt("MEGURI_MEASURE_HOSTS", 5000)
		per := envInt("MEGURI_MEASURE_PERHOST", 200)
		seeds = syntheticSeedsDelay0(hosts, per)
	}

	ctx := context.Background()
	fr := stubFetcher{}

	// Pass A: clean throughput, no per-call timer in the loop.
	fa := seedAll(New(1, 0), seeds)
	now := uint32(0)
	selA := 0
	startA := time.Now()
	for {
		req, ok := fa.Dispatch(now)
		if ok {
			selA++
			o, _ := fr.Fetch(ctx, req)
			fa.Report(o, now)
			continue
		}
		nt, ok := fa.NextEligible()
		if !ok || nt <= now {
			break
		}
		now = nt
	}
	elapsedA := time.Since(startA)

	// Pass B: per-call latency. The timer floor (the cost of the two time.Now calls
	// around a no-op) is calibrated and reported, so the reader can read the
	// distribution net of the measurement overhead, which at this latency is a real
	// fraction of the per-call cost.
	fb := seedAll(New(1, 0), seeds)
	lat := make([]int32, 0, len(seeds))
	now = 0
	for {
		t0 := time.Now()
		req, ok := fb.Dispatch(now)
		d := time.Since(t0).Nanoseconds()
		if ok {
			lat = append(lat, int32(d))
			o, _ := fr.Fetch(ctx, req)
			fb.Report(o, now)
			continue
		}
		nt, ok := fb.NextEligible()
		if !ok || nt <= now {
			break
		}
		now = nt
	}
	timerFloor := calibrateTimerNs()

	slices.Sort(lat)
	q := func(p float64) int32 {
		if len(lat) == 0 {
			return 0
		}
		i := int(p * float64(len(lat)))
		if i >= len(lat) {
			i = len(lat) - 1
		}
		return lat[i]
	}
	var sum int64
	for _, v := range lat {
		sum += int64(v)
	}
	mean := float64(sum) / float64(max(len(lat), 1))

	out := map[string]any{
		"urls":           len(seeds),
		"selections":     selA,
		"sel_per_sec":    float64(selA) / elapsedA.Seconds(),
		"clean_ns_per":   elapsedA.Seconds() * 1e9 / float64(max(selA, 1)),
		"timer_floor_ns": timerFloor,
		"lat_ns_p50":     q(0.50),
		"lat_ns_p90":     q(0.90),
		"lat_ns_p99":     q(0.99),
		"lat_ns_p999":    q(0.999),
		"lat_ns_max":     lat[len(lat)-1],
		"lat_ns_min":     lat[0],
		"lat_ns_mean":    mean,
	}
	b, _ := json.Marshal(out)
	t.Logf("DISPATCH_LATENCY %s", b)
}

// syntheticSeedsDelay0 is syntheticSeeds with a zero crawl delay, so the dispatch
// profile never blocks on politeness and measures the selection cost alone.
func syntheticSeedsDelay0(hosts, perHost int) []seed {
	return syntheticSeeds(hosts, perHost, 0)
}

// calibrateTimerNs measures the floor cost of the two time.Now calls that bracket
// each timed Dispatch, so the latency profile can be read net of its own overhead.
func calibrateTimerNs() float64 {
	const n = 1 << 20
	var acc int64
	for range n {
		t0 := time.Now()
		acc += time.Since(t0).Nanoseconds()
	}
	return float64(acc) / float64(n)
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
