package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// crawlOnce dispatches the single ready URL at clock now and reports a clean 200,
// returning the key that went out. It fails the test if nothing dispatched.
func crawlOnce(t *testing.T, f *Frontier, now uint32) meguri.URLKey {
	t.Helper()
	req, ok := f.Dispatch(now)
	if !ok {
		t.Fatalf("nothing dispatched at now=%d", now)
	}
	f.Report(meguri.Outcome{
		URLKey:     req.URLKey,
		HTTPStatus: 200,
		FetchedAt:  now / 3600,
		ContentFP:  req.URLKey.PathKey | 1,
	}, now)
	return req.URLKey
}

// TestScheduleIndexDefersRecrawl checks that with the schedule index on, a crawled
// URL does not re-dispatch immediately but re-enters the schedule as DueRecrawl
// when its NextDue hour arrives, so recrawl runs in the live loop instead of a
// crawl being terminal.
func TestScheduleIndexDefersRecrawl(t *testing.T) {
	f := New(1, 0, WithScheduleIndex(), WithStateMachine())
	host := "recrawl.test"
	f.Seed("http://"+host+"/p", host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/p")

	if got := crawlOnce(t, f, 0); got != key {
		t.Fatalf("first dispatch was %v, want %v", got, key)
	}
	rec := f.records[key]
	if rec.Status != meguri.StatusCrawled {
		t.Fatalf("status after crawl = %v, want Crawled", rec.Status)
	}
	if rec.NextDue != recrawlGapHours {
		t.Fatalf("next_due = %d, want %d (the flat recrawl gap from hour 0)", rec.NextDue, recrawlGapHours)
	}

	// Right after the crawl nothing is due: the URL waits in the wheel, not the
	// front bank, so a dispatch at the same instant finds no work.
	if _, ok := f.Dispatch(0); ok {
		t.Fatal("dispatched again immediately; a crawled URL must wait for its recrawl hour")
	}
	// The wheel is the earliest event, so the scheduler advances to the recrawl.
	due, ok := f.NextEligible()
	if !ok {
		t.Fatal("NextEligible reported drained, but a recrawl is pending in the wheel")
	}
	if want := uint32(recrawlGapHours) * 3600; due != want {
		t.Fatalf("NextEligible = %d, want %d (the recrawl hour in epoch-seconds)", due, want)
	}

	// At the recrawl hour the URL comes back as DueRecrawl and dispatches again.
	req, ok := f.Dispatch(due)
	if !ok {
		t.Fatal("nothing dispatched at the recrawl hour")
	}
	if req.URLKey != key {
		t.Fatalf("recrawl dispatched %v, want %v", req.URLKey, key)
	}
	if rec.CrawlCount != 1 {
		t.Fatalf("crawl_count = %d before the recrawl completes, want 1", rec.CrawlCount)
	}
}

// TestScheduleIndexDefersFutureSeed checks a seed dated into the future waits in
// the wheel until its hour rather than dispatching at once, the other half of the
// index: deferral on the way in, not only on recrawl.
func TestScheduleIndexDefersFutureSeed(t *testing.T) {
	f := New(1, 0, WithScheduleIndex())
	host := "later.test"
	const dueHour = 100
	f.Seed("http://"+host+"/q", host, 0.9, 0, dueHour, 10)
	key := meguri.MakeURLKey(host, "/q")

	if _, ok := f.Dispatch(0); ok {
		t.Fatal("dispatched a future-dated seed at hour 0; it must wait for its due hour")
	}
	if f.urlFront.len() != 0 {
		t.Fatalf("future seed sits in the front bank (%d), want it deferred in the wheel", f.urlFront.len())
	}
	req, ok := f.Dispatch(dueHour * 3600)
	if !ok {
		t.Fatal("nothing dispatched at the seed's due hour")
	}
	if req.URLKey != key {
		t.Fatalf("dispatched %v at the due hour, want %v", req.URLKey, key)
	}
}

// TestScheduleIndexSurvivesRecover checks the wheel survives a restart two ways:
// the checkpoint now serializes the durable timing-wheel region (decision D13, so a
// cold reader can pushdown), and Recover still rebuilds the resident wheel from the
// URL table, so recrawl re-enters the schedule at its NextDue hour either way. A
// crawled URL with a pending recrawl, checkpointed and recovered, still re-dispatches
// at the due hour.
func TestScheduleIndexSurvivesRecover(t *testing.T) {
	f := New(1, 0, WithScheduleIndex(), WithStateMachine())
	host := "persist.test"
	f.Seed("http://"+host+"/r", host, 0.9, 0, 0, 10)
	key := meguri.MakeURLKey(host, "/r")
	crawlOnce(t, f, 0) // now Crawled with NextDue = recrawlGapHours, pending in the wheel

	blob, err := f.CheckpointBytes()
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// The wheel-on checkpoint now carries the durable schedule region (D13): the
	// cold read path can find due work without recovering the frontier.
	rd, err := format.NewReader(blob)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if !rd.HasSchedule() {
		t.Fatal("wheel-on checkpoint did not serialize the durable schedule region")
	}
	p, err := format.Decode(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	g := Recover(p, WithScheduleIndex(), WithStateMachine())

	if rec := g.records[key]; rec == nil || rec.Status != meguri.StatusCrawled {
		t.Fatalf("recovered record = %+v, want a Crawled URL", rec)
	}
	if _, ok := g.Dispatch(0); ok {
		t.Fatal("recovered frontier dispatched before the recrawl hour")
	}
	req, ok := g.Dispatch(recrawlGapHours * 3600)
	if !ok {
		t.Fatal("recovered frontier did not recrawl at the due hour: the wheel was not rebuilt")
	}
	if req.URLKey != key {
		t.Fatalf("recovered recrawl dispatched %v, want %v", req.URLKey, key)
	}
}
