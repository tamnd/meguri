package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/freshness"
)

// keyFor resolves a seeded URL back to its record key, so a test can craft
// outcomes for a specific URL without going through dispatch.
func keyFor(host, url string) meguri.URLKey {
	return meguri.URLKey{HostKey: meguri.HostKeyOf(host), PathKey: meguri.PathKeyOf(PathOf(url))}
}

// report is one crawl observation at a given hour, with a content fingerprint and
// a simhash. The frontier clock for politeness is epoch-seconds and the data
// model clock is epoch-hours, so the report time is the hour scaled to seconds,
// keeping the two coherent the way the production loop does.
func report(f *Frontier, key meguri.URLKey, hour uint32, fp, sim uint64) {
	f.Report(meguri.Outcome{
		URLKey:     key,
		HTTPStatus: 200,
		FetchedAt:  hour,
		ContentFP:  fp,
		Simhash:    sim,
	}, hour*3600)
}

// TestFreshnessOffKeepsM1Schedule pins the opt-in contract: with the rescheduler
// off, a crawled URL keeps the flat M1 placeholder next-due and never gets a
// change-rate estimate, so the M1 through M3 dispatch behavior is unchanged.
func TestFreshnessOffKeepsM1Schedule(t *testing.T) {
	f := New(1, 0)
	f.Seed("http://a.test/x", "a.test", 0.5, 0, 0, 10)
	key := keyFor("a.test", "http://a.test/x")

	report(f, key, 1000, 0x11, 0x22)

	rec := f.records[key]
	if rec.Status != meguri.StatusCrawled {
		t.Errorf("status = %v, want Crawled (M1 leaves a crawled URL crawled)", rec.Status)
	}
	if rec.NextDue != 1000+recrawlGapHours {
		t.Errorf("next_due = %d, want the flat M1 placeholder %d", rec.NextDue, 1000+recrawlGapHours)
	}
	if rec.Lambda != 0 {
		t.Errorf("lambda = %v, want 0 with the rescheduler off", rec.Lambda)
	}
}

// TestFreshnessOnReschedulesCrawledURL checks the wiring end to end: with the
// rescheduler on, a crawl flows the change counters into the Poisson estimate,
// the estimate into the allocation, and the allocation into next_due and the
// DueRecrawl status, replacing the flat M1 placeholder.
func TestFreshnessOnReschedulesCrawledURL(t *testing.T) {
	p := freshness.DefaultParams()
	f := New(1, 0, WithFreshness(p, 1000))
	f.Seed("http://a.test/x", "a.test", 0.8, 0, 0, 10)
	key := keyFor("a.test", "http://a.test/x")

	// One crawl: no interval yet, so the estimate holds at the floor, but the URL
	// must still move onto a freshness schedule rather than the M1 placeholder.
	report(f, key, 100, 0x11, 0x22)

	rec := f.records[key]
	if rec.Status != meguri.StatusDueRecrawl {
		t.Errorf("status = %v, want DueRecrawl (the rescheduler owns the URL)", rec.Status)
	}
	if rec.NextDue == 100+recrawlGapHours {
		t.Errorf("next_due = %d is still the flat M1 placeholder; the rescheduler did not run", rec.NextDue)
	}
	if rec.Lambda <= 0 {
		t.Errorf("lambda = %v, want the floor at least", rec.Lambda)
	}
}

// TestFreshnessEstimateDivergesOnRealHistory drives two URLs through repeated
// crawls, one changing every interval and one never changing, and checks the
// wired-in estimator separates them: the busy page earns a change rate well above
// the quiet one, and the busy page is scheduled sooner.
func TestFreshnessEstimateDivergesOnRealHistory(t *testing.T) {
	p := freshness.DefaultParams()
	f := New(1, 0, WithFreshness(p, 1000))
	f.Seed("http://a.test/fast", "a.test", 0.8, 0, 0, 10)
	f.Seed("http://b.test/slow", "b.test", 0.8, 0, 0, 10)
	fast := keyFor("a.test", "http://a.test/fast")
	slow := keyFor("b.test", "http://b.test/slow")

	// Six crawls 24 hours apart. The fast page flips its simhash between two
	// Hamming-distant values every crawl, a real content change each interval; the
	// slow page reports the same fingerprint every time, a no-change each interval.
	for i := range uint32(6) {
		hour := i * 24
		var fastSim uint64
		if i%2 == 1 {
			fastSim = ^uint64(0) // 64 bits away from zero, well past the cosmetic gate
		}
		report(f, fast, hour, 0x100+uint64(i), fastSim)
		report(f, slow, hour, 0x7, 0x0)
	}

	rf := f.records[fast]
	rs := f.records[slow]
	if !(rf.Lambda > rs.Lambda) {
		t.Fatalf("estimator did not separate the pages: fast lambda=%v slow lambda=%v", rf.Lambda, rs.Lambda)
	}
	if float64(rs.Lambda) > 0.01 {
		t.Errorf("a never-changed page got an implausible rate %v", rs.Lambda)
	}
	if rf.ChangeCount == 0 {
		t.Fatalf("the fast page recorded no changes; the simhash gate swallowed them")
	}
	// The busier page comes due sooner: a shorter recrawl interval from a higher
	// change rate, the whole point of the rescheduler.
	if rf.NextDue > rs.NextDue {
		t.Errorf("fast page not scheduled at least as soon as the slow page: fast due=%d slow due=%d", rf.NextDue, rs.NextDue)
	}
}
