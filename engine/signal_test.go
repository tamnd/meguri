package engine

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/distribute"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/prioritize"
)

// recSink records the entries a partition imports so the engine test can assert
// which slice of a routed bundle reached each owner.
type recSink struct {
	hosts map[uint64]float32
	urls  map[meguri.URLKey]float32
}

func newRecSink() *recSink {
	return &recSink{hosts: map[uint64]float32{}, urls: map[meguri.URLKey]float32{}}
}

func (s *recSink) ImportHostSignal(h meguri.HostSignal) { s.hosts[h.HostKey] = h.HostScore }
func (s *recSink) ImportURLSignal(u meguri.URLSignal)   { s.urls[u.URLKey] = u.PageRank }

// TestImportSignalRoutesToOwners runs the producer side of tsumugi import through
// the engine: partition 0 reads a full bundle and calls ImportSignal, which
// splits it by owner, applies its own slice to its frontier, and ships partition
// 1 its slice over the signal transport. Partition 1 then drains the transport and
// the entries it owns, and only those, arrive.
func TestImportSignalRoutesToOwners(t *testing.T) {
	mp := &distribute.Map{Epoch: 1, NumPartitions: 2}
	h0, h1 := twoHostsAcrossPartitions(mp)
	if h0 == "" || h1 == "" {
		t.Fatal("could not find hosts on both partitions")
	}
	hk0, hk1 := meguri.HostKeyOf(h0), meguri.HostKeyOf(h1)
	remoteURL := meguri.MakeURLKey(h1, "/a")

	tr := distribute.NewChannelSignalTransport(8)
	sr0 := distribute.NewSignalRouter(0, mp, tr)
	sr1 := distribute.NewSignalRouter(1, mp, tr)

	fr0 := frontier.New(0, 0, frontier.WithPrioritizer(prioritize.DefaultParams()))
	e0 := New(fr0, Config{Fetcher: &recFetcher{clk: NewLogicalClock(0)}, Signals: sr0})

	bundle := meguri.Signal{
		Epoch: 3,
		Hosts: []meguri.HostSignal{{HostKey: hk0, HostScore: 0.4}, {HostKey: hk1, HostScore: 0.8}},
		URLs:  []meguri.URLSignal{{URLKey: remoteURL, PageRank: 2.0}},
	}
	if err := e0.ImportSignal(bundle); err != nil {
		t.Fatalf("import: %v", err)
	}

	sink1 := newRecSink()
	if n := sr1.Apply(sink1); n != 1 {
		t.Fatalf("partition 1 applied %d bundles, want 1", n)
	}
	if got, ok := sink1.hosts[hk1]; !ok || got != 0.8 {
		t.Fatalf("partition 1 host score = %v (present %v), want 0.8", got, ok)
	}
	if _, leaked := sink1.hosts[hk0]; leaked {
		t.Fatal("partition 0's host score leaked to partition 1")
	}
	if got, ok := sink1.urls[remoteURL]; !ok || got != 2.0 {
		t.Fatalf("partition 1 url signal = %v (present %v), want 2.0", got, ok)
	}
}
