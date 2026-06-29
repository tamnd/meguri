package frontier

import (
	"context"
	"os"
	"testing"

	"github.com/tamnd/meguri"
)

// TestEffectiveTargetFloor checks the k*threads back-queue floor (doc 05): a
// dispatch-thread count raises the effective active-host target above a tighter
// memory cap, so the fetcher pool never idles for want of a ready host, while an
// unbounded default frontier keeps its unbounded target and stays unbounded.
func TestEffectiveTargetFloor(t *testing.T) {
	def := New(1, 0)
	if def.bounded {
		t.Fatal("default frontier should not be bounded")
	}
	if got := def.effectiveTarget(); got != defaultTarget {
		t.Fatalf("default effectiveTarget = %d, want %d", got, defaultTarget)
	}

	f := New(1, 0, WithTarget(2), WithDispatchThreads(4))
	if !f.bounded {
		t.Fatal("WithTarget/WithDispatchThreads should mark the frontier bounded")
	}
	want := backQueuesPerThread * 4 // 12, above the memory cap of 2
	if got := f.effectiveTarget(); got != want {
		t.Fatalf("effectiveTarget = %d, want %d (k*threads floor over the cap)", got, want)
	}
}

// TestBackQueueFloorActivatesHosts checks the floor actually changes dispatch:
// with a memory cap of 2 but four dispatch threads, distribute activates k*threads
// hosts, not two, so every thread has a back queue to pull from.
func TestBackQueueFloorActivatesHosts(t *testing.T) {
	f := New(1, 0, WithTarget(2), WithDispatchThreads(4))
	seedAll(f, syntheticSeeds(20, 1, 10))
	f.distribute(0)
	if got, want := f.active, backQueuesPerThread*4; got != want {
		t.Fatalf("active hosts = %d, want %d (floored at k*threads)", got, want)
	}
}

// TestPromoteAgedLiftsStarvedURL checks the anti-starvation sweep promotes a
// front-bank URL aged past the threshold to the top bucket, so the next free
// active-host slot takes it ahead of fresher, higher-scored work.
func TestPromoteAgedLiftsStarvedURL(t *testing.T) {
	f := New(1, 0, WithTarget(1))
	f.Seed("http://low.example/x", "low.example", 0.1, 0, 0, 10)
	key := meguri.MakeURLKey("low.example", PathOf("http://low.example/x"))

	if got := frontBucket(0.1); got == priorityLevels-1 {
		t.Fatal("test setup: low priority should not already be the top bucket")
	}
	f.promoteAged(frontAgePromoteHours + 1)
	if got := f.records[key].Priority; got != 1.0 {
		t.Fatalf("promoted priority = %v, want 1.0", got)
	}
	peeked, ok := f.urlFront.peek()
	if !ok || peeked != key {
		t.Fatalf("promoted URL is not at the front of the bank")
	}
}

// TestPromoteAgedSkipsFresh checks a URL that has not waited past the threshold is
// left untouched, so promotion is a starvation backstop, not a blanket reset.
func TestPromoteAgedSkipsFresh(t *testing.T) {
	f := New(1, 0, WithTarget(1))
	f.Seed("http://fresh.example/x", "fresh.example", 0.1, 100, 0, 10) // enqueued at hour 100
	key := meguri.MakeURLKey("fresh.example", PathOf("http://fresh.example/x"))
	f.promoteAged(100 + frontAgePromoteHours - 1) // waited one hour short of the threshold
	if got := f.records[key].Priority; got != 0.1 {
		t.Fatalf("fresh priority = %v, want 0.1 (not promoted)", got)
	}
}

// TestAgeSweepRunsOnCadence drives dispatch with the clock past the threshold and
// checks the sweep fires on the frontRefillBatch cadence, promoting a host the
// active-host cap has kept out of the running. The two high hosts hold both slots
// across this whole run, so the starved host never binds; the test asserts only
// that it was promoted, which is the wiring the cadence provides.
func TestAgeSweepRunsOnCadence(t *testing.T) {
	f := New(1, 0, WithTarget(2))
	for i := range 60 {
		f.Seed("http://hi-a.example/"+itoa(i), "hi-a.example", 0.9, 0, 0, 10)
		f.Seed("http://hi-b.example/"+itoa(i), "hi-b.example", 0.9, 0, 0, 10)
	}
	f.Seed("http://low.example/x", "low.example", 0.05, 0, 0, 10)
	lowKey := meguri.MakeURLKey("low.example", PathOf("http://low.example/x"))

	now := uint32(200 * 3600) // well past frontAgePromoteHours (168) of waiting
	// Run at least a full frontRefillBatch of dispatch calls so the sweep fires.
	// When politeness parks both active hosts, advance the clock by a second (still
	// inside hour 200) rather than stopping, so distribute keeps running.
	for range frontRefillBatch + 4 {
		req, ok := f.Dispatch(now)
		if ok {
			f.Report(meguri.Outcome{URLKey: req.URLKey, HTTPStatus: 200, FetchedAt: now, ContentFP: 1}, now)
		} else {
			now++
		}
	}
	if got := f.records[lowKey].Priority; got != 1.0 {
		t.Fatalf("starved host priority = %v after the sweep cadence, want 1.0", got)
	}
}

// TestUnboundedSkipsAgeBookkeeping checks an unbounded frontier records no
// front-bank waits, so the anti-starvation tier adds nothing to the common case
// and the earlier milestones' dispatch path is byte-for-byte unchanged.
func TestUnboundedSkipsAgeBookkeeping(t *testing.T) {
	f := seedAll(New(1, 0), syntheticSeeds(8, 4, 10))
	if len(f.frontAge) != 0 {
		t.Fatalf("unbounded frontier recorded %d front-bank waits, want 0", len(f.frontAge))
	}
}

// TestCorpusBoundedNoStarvation runs a bounded frontier with the k*threads floor
// over the real slice and checks every seeded host is still crawled: finite work
// drains, so the bound delays a host but never abandons it, and the new dispatch
// path runs on real data. It skips when no corpus is configured.
func TestCorpusBoundedNoStarvation(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice")
	}
	seeds := loadCorpusSeeds(t, path)
	f := New(1, 0, WithTarget(4), WithDispatchThreads(2)) // floor 6 active hosts
	want := map[uint64]bool{}
	for _, s := range seeds {
		f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
		want[meguri.HostKeyOf(s.host)] = true
	}
	d, err := f.Drain(context.Background(), 0, stubFetcher{})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	got := map[uint64]bool{}
	for _, x := range d {
		got[x.HostKey] = true
	}
	for hk := range want {
		if !got[hk] {
			t.Fatalf("host %d seeded but never dispatched under a bounded frontier", hk)
		}
	}
}
