package frontier

import (
	"fmt"
	"testing"

	"github.com/tamnd/meguri"
)

// calDisc builds a discovery for a future-dated calendar URL on host, the trap
// signature the detector accumulates toward HostFlagTrapSuspect.
func calDisc(host, url string) meguri.Discovery {
	return meguri.Discovery{
		URLKey:          keyFor(host, url),
		CanonicalURL:    url,
		Depth:           1,
		DiscoverySource: meguri.SourceLink,
	}
}

// TestDiscoverTrapHeuristics exercises the trap detector through the discovery
// path (doc 08, section 8.4): a stream of distinct future-dated calendar URLs on
// one host flags it a trap suspect, after which further future-dated URLs are
// parked in Trapped while a clean URL on the same host is still scheduled. The
// host with an unlimited budget and depth cap isolates the precise pattern defense
// from the blunt one, so only the heuristic can park a URL here.
func TestDiscoverTrapHeuristics(t *testing.T) {
	f := New(1, 0)
	const host = "events.example.org"
	// A campaign clock in mid-2026 (epoch-seconds), so a 2031 URL is past the horizon.
	const nowSec = uint32(1780000000)

	// Feed sixteen distinct future months (rolling the year so every month is valid):
	// each is admitted while the host is not yet a suspect, and the sixteenth crosses
	// the default threshold.
	hk := meguri.HostKeyOf(host)
	for i := range 16 {
		url := fmt.Sprintf("https://%s/%d/%02d/", host, 2031+i/12, i%12+1)
		f.Discover(calDisc(host, url), nowSec)
	}
	if f.hosts[hk] == nil || f.hosts[hk].rec.Flags&meguri.HostFlagTrapSuspect == 0 {
		t.Fatal("host not flagged a trap suspect after a stream of future-dated URLs")
	}

	// A further future-dated URL is now parked, not scheduled.
	future := fmt.Sprintf("https://%s/2032/06/", host)
	if f.Discover(calDisc(host, future), nowSec) {
		t.Fatal("a future-dated URL was scheduled on a flagged host, want it parked")
	}
	if rec := f.records[keyFor(host, future)]; rec == nil || rec.Status != meguri.StatusTrapped {
		t.Fatalf("future-dated URL status = %v, want Trapped", recStatus(f, host, future))
	}

	// A clean (non-dated) URL on the same flagged host is still admitted, because the
	// host was flagged only for the calendar rule.
	clean := fmt.Sprintf("https://%s/about/contact", host)
	if !f.Discover(calDisc(host, clean), nowSec) {
		t.Fatal("a clean URL was parked on a calendar-flagged host, want it scheduled")
	}
	if rec := f.records[keyFor(host, clean)]; rec == nil || rec.Status != meguri.StatusScheduled {
		t.Fatalf("clean URL status = %v, want Scheduled", recStatus(f, host, clean))
	}
}

// TestDiscoverNoTrapForHealthyHost pins the opt-in contract: a host that never
// trips a pattern rule is never flagged and admits every discovery, so the trap
// detector does not disturb the earlier milestones' admission on healthy hosts.
func TestDiscoverNoTrapForHealthyHost(t *testing.T) {
	f := New(1, 0)
	const host = "blog.example.org"
	const nowSec = uint32(1780000000)

	for i := range 40 {
		url := fmt.Sprintf("https://%s/posts/%d", host, i)
		if !f.Discover(calDisc(host, url), nowSec) {
			t.Fatalf("healthy host parked discovery %d, want all scheduled", i)
		}
	}
	if h := f.hosts[meguri.HostKeyOf(host)]; h != nil && h.rec.Flags&meguri.HostFlagTrapSuspect != 0 {
		t.Fatal("healthy host wrongly flagged a trap suspect")
	}
}

func recStatus(f *Frontier, host, url string) any {
	if rec := f.records[keyFor(host, url)]; rec != nil {
		return rec.Status
	}
	return "no record"
}
