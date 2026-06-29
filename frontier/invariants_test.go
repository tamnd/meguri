package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
)

// scheduleResident collects the keys resident in the dispatch schedule: the front
// bank of URLs not yet bound to a host, plus every host back queue. It is the M1
// schedule invariant 105 is stated over, evaluated at a quiescent point (no URL in
// flight, so a back-queue head is a waiting URL, not a dispatched one).
func scheduleResident(f *Frontier) map[meguri.URLKey]bool {
	s := map[meguri.URLKey]bool{}
	for _, bucket := range f.urlFront.buckets {
		for _, k := range bucket {
			s[k] = true
		}
	}
	for _, h := range f.hosts {
		for _, k := range h.back {
			s[k] = true
		}
	}
	return s
}

// assertScheduleMatchesStatus checks invariant 105 at a quiescent point: a record
// is resident in the dispatch schedule exactly when its status is Scheduled or
// DueRecrawl, and no other status is resident.
func assertScheduleMatchesStatus(t *testing.T, f *Frontier) {
	t.Helper()
	resident := scheduleResident(f)
	for key, rec := range f.records {
		want := rec.Status == meguri.StatusScheduled || rec.Status == meguri.StatusDueRecrawl
		if got := resident[key]; got != want {
			t.Fatalf("key %v status %v: scheduled=%v, want %v (in schedule iff Scheduled or DueRecrawl)", key, rec.Status, got, want)
		}
	}
	// Nothing resident may lack a record.
	for key := range resident {
		if f.records[key] == nil {
			t.Fatalf("key %v resident in the schedule with no record", key)
		}
	}
}

// TestHostRecordAutoCreatedWithFirstURL pins invariant 103: the first URL of a host
// auto-creates its HostRecord, and later URLs of the same host reuse it and bump its
// url_count rather than creating a second. Both intake paths, Seed and Discover,
// hold the invariant.
func TestHostRecordAutoCreatedWithFirstURL(t *testing.T) {
	f := New(1, 0)

	// First URL of host a: its host record appears.
	f.Seed("http://a.test/1", "a.test", 0.5, 0, 0, 10)
	ak := meguri.HostKeyOf("a.test")
	if len(f.hosts) != 1 {
		t.Fatalf("after first seed, host count = %d, want 1", len(f.hosts))
	}
	if h := f.hosts[ak]; h == nil || h.rec.URLCount != 1 {
		t.Fatalf("host a record = %+v, want url_count 1", f.hosts[ak])
	}

	// Second URL of host a: same record, url_count rises, no new host.
	f.Seed("http://a.test/2", "a.test", 0.5, 0, 0, 10)
	if len(f.hosts) != 1 {
		t.Fatalf("after a second url on host a, host count = %d, want 1", len(f.hosts))
	}
	if f.hosts[ak].rec.URLCount != 2 {
		t.Fatalf("host a url_count = %d, want 2", f.hosts[ak].rec.URLCount)
	}

	// First URL of host b arriving as a discovery also auto-creates its host.
	f.Discover(disc("http://b.test/x", "b.test", ak), 0)
	bk := meguri.HostKeyOf("b.test")
	if len(f.hosts) != 2 {
		t.Fatalf("after discovering host b, host count = %d, want 2", len(f.hosts))
	}
	if f.hosts[bk] == nil {
		t.Fatal("discovering the first url of host b did not auto-create its host record")
	}
}

// TestScheduleMembershipMatchesStatus pins invariant 105 across the lifecycle: a URL
// is resident in the dispatch schedule exactly while its status is Scheduled or
// DueRecrawl. Seeding makes it Scheduled and resident; dispatching and crawling it
// makes it Crawled and not resident; a recrawl promotion flips it to DueRecrawl and
// puts it back. The invariant is checked at every quiescent point.
func TestScheduleMembershipMatchesStatus(t *testing.T) {
	f := New(1, 0, WithStateMachine(), WithScheduleIndex())

	// Seed three URLs across two hosts: all Scheduled, all resident.
	f.Seed("http://a.test/1", "a.test", 0.9, 0, 0, 0)
	f.Seed("http://a.test/2", "a.test", 0.5, 0, 0, 0)
	f.Seed("http://b.test/1", "b.test", 0.7, 0, 0, 0)
	assertScheduleMatchesStatus(t, f)

	// Crawl every URL to completion, advancing the clock across politeness windows
	// but stopping before the recrawl hour, so the only work left is the future
	// recrawls in the wheel. Each URL becomes Crawled and leaves the dispatch
	// schedule (it parks in the wheel, which is the due-time index, not the schedule).
	recrawlNow := uint32(recrawlGapHours) * 3600
	now := uint32(0)
	for {
		req, ok := f.Dispatch(now)
		if !ok {
			t, has := f.NextEligible()
			if !has || t >= recrawlNow {
				break // only future recrawls remain
			}
			now = t
			continue
		}
		f.Report(meguri.Outcome{
			URLKey:     req.URLKey,
			HTTPStatus: 200,
			FetchedAt:  now / 3600,
			ContentFP:  req.URLKey.PathKey | 1,
		}, now)
	}
	assertScheduleMatchesStatus(t, f)
	for key, rec := range f.records {
		if rec.Status != meguri.StatusCrawled {
			t.Fatalf("after draining, key %v status %v, want Crawled", key, rec.Status)
		}
	}

	// Advance to the recrawl hour and fire the wheel without dispatching: the due
	// URLs flip to DueRecrawl and re-enter the schedule, which the invariant must see.
	f.promoteDue(recrawlNow)
	due := 0
	for _, rec := range f.records {
		if rec.Status == meguri.StatusDueRecrawl {
			due++
		}
	}
	if due == 0 {
		t.Fatal("no URL flipped to DueRecrawl at the recrawl hour; the recrawl never re-entered the schedule")
	}
	assertScheduleMatchesStatus(t, f)
}
