package engine

import (
	"context"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/distribute"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/prioritize"
)

// linkFetcher crawls the seeded URL and, exactly once per URL, returns a single
// out-link to a fixed target so the test can watch the link route to its owner.
type linkFetcher struct {
	target meguri.Discovery
	fired  map[meguri.URLKey]bool
}

func (f *linkFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	o := meguri.Outcome{URLKey: req.URLKey, HTTPStatus: 200, FetchedAt: 500}
	if f.fired == nil {
		f.fired = map[meguri.URLKey]bool{}
	}
	if !f.fired[req.URLKey] {
		f.fired[req.URLKey] = true
		o.Links = []meguri.Discovery{f.target}
	}
	return o, nil
}

// twoHostsAcrossPartitions finds two host strings that a 2-partition jump-hash map
// assigns to partition 0 and partition 1, so the routing test has a genuine local
// host and a genuine remote one.
func twoHostsAcrossPartitions(m *distribute.Map) (h0, h1 string) {
	for i := 0; h0 == "" || h1 == ""; i++ {
		name := "host" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + ".example"
		switch m.Owner(meguri.HostKeyOf(name)) {
		case 0:
			if h0 == "" {
				h0 = name
			}
		case 1:
			if h1 == "" {
				h1 = name
			}
		}
		if i > 5000 {
			break
		}
	}
	return h0, h1
}

// TestEngineRoutesCrossPartitionLinks runs the full distribution fold through the
// engine: partition 0 crawls a local seed whose out-link belongs to partition 1,
// the frontier's link sink ships it through the router, and partition 1's engine
// drains its inbound transport and schedules it. This closes the doc-04 intake
// item and the doc-12 route-through-the-engine path.
func TestEngineRoutesCrossPartitionLinks(t *testing.T) {
	mp := &distribute.Map{Epoch: 1, NumPartitions: 2}
	h0, h1 := twoHostsAcrossPartitions(mp)
	if h0 == "" || h1 == "" {
		t.Fatal("could not find hosts on both partitions")
	}
	tr := distribute.NewChannelTransport(64)
	r0 := distribute.NewRouter(0, mp, tr, 16)
	r1 := distribute.NewRouter(1, mp, tr, 16)

	// The out-link partition 0's crawl emits, owned by partition 1.
	target := meguri.Discovery{
		URLKey:          meguri.URLKey{HostKey: meguri.HostKeyOf(h1), PathKey: meguri.PathKeyOf("/landing")},
		CanonicalURL:    "https://" + h1 + "/landing",
		Depth:           1,
		DiscoverySource: meguri.SourceLink,
		LinkWeight:      0.5,
	}

	fr0 := frontier.New(0, 0,
		frontier.WithPrioritizer(prioritize.DefaultParams()),
		frontier.WithLinkRouter(RouteSink(r0)),
	)
	fr0.Seed("https://"+h0+"/", h0, 0.9, 0, 0, 10)

	clk := NewLogicalClock(1000)
	eng0 := New(fr0, Config{Fetcher: &linkFetcher{target: target}, Workers: 2, Clock: clk, Router: r0, UntilEmpty: true})
	if err := eng0.Run(context.Background()); err != nil {
		t.Fatalf("run p0: %v", err)
	}

	// Partition 1 starts empty; its engine drains the inbound transport, schedules
	// the routed link, and dispatches it.
	fr1 := frontier.New(1, 0, frontier.WithPrioritizer(prioritize.DefaultParams()))
	rf := &recFetcher{clk: NewLogicalClock(2000)}
	eng1 := New(fr1, Config{Fetcher: rf, Workers: 2, Clock: rf.clk, Router: r1, UntilEmpty: true})
	if err := eng1.Run(context.Background()); err != nil {
		t.Fatalf("run p1: %v", err)
	}

	if len(rf.seq) != 1 {
		t.Fatalf("partition 1 dispatched %d urls, want the 1 routed link", len(rf.seq))
	}
	if rf.seq[0].key != target.URLKey {
		t.Fatalf("partition 1 dispatched %x, want the routed link %x", rf.seq[0].key, target.URLKey)
	}
}
