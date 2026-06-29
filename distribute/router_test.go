package distribute

import (
	"testing"

	m "github.com/tamnd/meguri"
)

func disc(hostKey, pathKey uint64) m.Discovery {
	return m.Discovery{URLKey: m.URLKey{HostKey: hostKey, PathKey: pathKey}}
}

// TestRouterLocalVsRemote checks the router keeps the discoveries it owns and
// ships the rest, the basic split every outcome goes through.
func TestRouterLocalVsRemote(t *testing.T) {
	mp := &Map{Epoch: 1, NumPartitions: 4}
	tr := NewChannelTransport(64)
	r := NewRouter(0, mp, tr, 8)

	var links []m.Discovery
	var wantLocal int
	for hk := range uint64(400) {
		links = append(links, disc(hk, 1))
		if mp.Owner(hk) == 0 {
			wantLocal++
		}
	}
	local, err := r.RouteLinks(links)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if len(local) != wantLocal {
		t.Fatalf("local count %d, want %d", len(local), wantLocal)
	}
	for _, d := range local {
		if mp.Owner(d.URLKey.HostKey) != 0 {
			t.Fatalf("kept a discovery this partition does not own: host %d", d.URLKey.HostKey)
		}
	}
}

// TestRouterRoutesToOwner drains every other partition's inbound stream and
// checks each shipped discovery landed on exactly the partition that owns its
// host, the routing correctness the seen-set then dedups.
func TestRouterRoutesToOwner(t *testing.T) {
	mp := &Map{Epoch: 1, NumPartitions: 5}
	tr := NewChannelTransport(256)
	r := NewRouter(0, mp, tr, 4)

	var links []m.Discovery
	sent := 0
	for hk := range uint64(1000) {
		links = append(links, disc(hk, hk))
		if mp.Owner(hk) != 0 {
			sent++
		}
	}
	if _, err := r.RouteLinks(links); err != nil {
		t.Fatalf("route: %v", err)
	}
	got := 0
	for pid := PartitionID(1); pid < 5; pid++ {
		recv := NewRouter(pid, mp, tr, 4)
		for _, d := range recv.Drain() {
			if mp.Owner(d.URLKey.HostKey) != pid {
				t.Fatalf("discovery for host %d landed on %d, owner is %d", d.URLKey.HostKey, pid, mp.Owner(d.URLKey.HostKey))
			}
			got++
		}
	}
	if got != sent {
		t.Fatalf("delivered %d remote discoveries, sent %d", got, sent)
	}
}

// TestRouterBatchesByDestination checks the router ships one message per
// destination per call, not one per link, the volume reduction that makes the
// transport tractable at fleet scale. A counting transport records the messages.
func TestRouterBatchesByDestination(t *testing.T) {
	mp := &Map{Epoch: 1, NumPartitions: 6}
	ct := &countTransport{}
	r := NewRouter(0, mp, ct, 1<<30) // huge batch size, so only flushAll sends

	var links []m.Discovery
	dests := map[PartitionID]bool{}
	for hk := range uint64(600) {
		links = append(links, disc(hk, 1))
		if o := mp.Owner(hk); o != 0 {
			dests[o] = true
		}
	}
	if _, err := r.RouteLinks(links); err != nil {
		t.Fatalf("route: %v", err)
	}
	if ct.messages != len(dests) {
		t.Fatalf("sent %d messages for %d destinations, want one per destination", ct.messages, len(dests))
	}
}

// TestRouterSwapMapMonotone checks a router accepts a newer epoch and rejects a
// stale or equal one, so an out-of-order fetch never moves it backward.
func TestRouterSwapMapMonotone(t *testing.T) {
	r := NewRouter(0, &Map{Epoch: 5, NumPartitions: 2}, NewChannelTransport(1), 1)
	if r.SwapMap(&Map{Epoch: 5, NumPartitions: 9}) {
		t.Fatal("swapped in an equal-epoch map")
	}
	if r.SwapMap(&Map{Epoch: 4, NumPartitions: 9}) {
		t.Fatal("swapped in a stale-epoch map")
	}
	if !r.SwapMap(&Map{Epoch: 6, NumPartitions: 9}) {
		t.Fatal("rejected a newer-epoch map")
	}
	if r.Map().NumPartitions != 9 {
		t.Fatalf("router kept the old map after a valid swap")
	}
}

// countTransport counts Send calls and discards the payload, for asserting the
// message volume.
type countTransport struct{ messages int }

func (c *countTransport) Send(_ PartitionID, batch []m.Discovery) error {
	if len(batch) > 0 {
		c.messages++
	}
	return nil
}
func (c *countTransport) Recv(PartitionID) ([]m.Discovery, bool) { return nil, false }
