package distribute

import (
	"os"
	"testing"

	m "github.com/tamnd/meguri"
)

// recordingSink is the test binding of SignalSink: it records every entry it is
// handed so a test can assert which signals a partition actually imported.
type recordingSink struct {
	hosts map[uint64]float32
	urls  map[m.URLKey]float32
}

func newRecordingSink() *recordingSink {
	return &recordingSink{hosts: map[uint64]float32{}, urls: map[m.URLKey]float32{}}
}

func (s *recordingSink) ImportHostSignal(h m.HostSignal) { s.hosts[h.HostKey] = h.HostScore }
func (s *recordingSink) ImportURLSignal(u m.URLSignal)   { s.urls[u.URLKey] = u.PageRank }

// threePartMap is a three-partition map with no overrides, so ownership is pure
// jump hash over the HostKey.
func threePartMap() *Map {
	return &Map{Epoch: 1, NumPartitions: 3, Partitions: []PartitionMeta{
		{ID: 0}, {ID: 1}, {ID: 2},
	}}
}

// sampleBundle builds an import bundle spanning many hosts so the split lands
// entries on every partition.
func sampleBundle(epoch uint64, n int) (m.Signal, []string) {
	var s m.Signal
	s.Epoch = epoch
	names := make([]string, n)
	for i := range n {
		host := "h" + itoa(i) + ".example"
		names[i] = host
		hk := m.HostKeyOf(host)
		s.Hosts = append(s.Hosts, m.HostSignal{HostKey: hk, HostScore: float32(i) / float32(n)})
		uk := m.MakeURLKey(host, "/p"+itoa(i))
		s.URLs = append(s.URLs, m.URLSignal{URLKey: uk, PageRank: float32(i)})
	}
	return s, names
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// TestRouteSignalSplitsByOwner checks a full import bundle splits so each
// partition receives exactly the host and URL entries it owns, the local slice
// returned to the caller and every remote slice sent over the transport, with the
// epoch preserved on each.
func TestRouteSignalSplitsByOwner(t *testing.T) {
	mp := threePartMap()
	tr := NewChannelSignalTransport(4)
	r := NewSignalRouter(0, mp, tr)

	bundle, _ := sampleBundle(7, 60)
	local, err := r.RouteSignal(bundle)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if local.Epoch != 7 {
		t.Fatalf("local epoch = %d, want 7", local.Epoch)
	}

	// Apply the local slice and drain the two remote partitions into their own
	// sinks, then check every entry landed on its owner and nowhere else.
	sinks := map[PartitionID]*recordingSink{0: newRecordingSink(), 1: newRecordingSink(), 2: newRecordingSink()}
	r.ApplyLocal(local, sinks[0])
	for _, id := range []PartitionID{1, 2} {
		rr := NewSignalRouter(id, mp, tr)
		rr.Apply(sinks[id])
	}

	for _, h := range bundle.Hosts {
		owner := mp.Owner(h.HostKey)
		if _, ok := sinks[owner].hosts[h.HostKey]; !ok {
			t.Fatalf("host %d not imported by its owner %d", h.HostKey, owner)
		}
		for id, s := range sinks {
			if id != owner {
				if _, leaked := s.hosts[h.HostKey]; leaked {
					t.Fatalf("host %d leaked to non-owner %d", h.HostKey, id)
				}
			}
		}
	}
	for _, u := range bundle.URLs {
		owner := mp.Owner(u.URLKey.HostKey)
		if _, ok := sinks[owner].urls[u.URLKey]; !ok {
			t.Fatalf("url %v not imported by its owner %d", u.URLKey, owner)
		}
	}
}

// TestApplyEpochGuard checks a bundle at or below the highest epoch already
// applied is dropped whole, so an out-of-order or redelivered import never
// reverts fresher signal, while a strictly newer epoch is applied.
func TestApplyEpochGuard(t *testing.T) {
	mp := &Map{Epoch: 1, NumPartitions: 1, Partitions: []PartitionMeta{{ID: 0}}}
	tr := NewChannelSignalTransport(8)
	r := NewSignalRouter(0, mp, tr)
	sink := newRecordingSink()

	hk := m.HostKeyOf("only.example")
	send := func(epoch uint64, score float32) {
		if err := tr.SendSignal(0, m.Signal{Epoch: epoch, Hosts: []m.HostSignal{{HostKey: hk, HostScore: score}}}); err != nil {
			t.Fatal(err)
		}
	}

	send(5, 0.5)
	if n := r.Apply(sink); n != 1 {
		t.Fatalf("applied %d bundles for epoch 5, want 1", n)
	}
	send(3, 0.3) // stale, must be dropped
	if n := r.Apply(sink); n != 0 {
		t.Fatalf("applied %d bundles for stale epoch 3, want 0", n)
	}
	if got := sink.hosts[hk]; got != 0.5 {
		t.Fatalf("host score = %v after a stale import, want 0.5", got)
	}
	send(7, 0.7) // newer, must win
	if n := r.Apply(sink); n != 1 {
		t.Fatalf("applied %d bundles for epoch 7, want 1", n)
	}
	if got := sink.hosts[hk]; got != 0.7 {
		t.Fatalf("host score = %v after epoch 7, want 0.7", got)
	}
}

// TestApplyLocalGuardIdempotent checks the local slice applies once and a second
// ApplyLocal at the same epoch is a no-op, the same guard the inbound path uses.
func TestApplyLocalGuardIdempotent(t *testing.T) {
	mp := &Map{Epoch: 1, NumPartitions: 1, Partitions: []PartitionMeta{{ID: 0}}}
	r := NewSignalRouter(0, mp, NewChannelSignalTransport(2))
	sink := newRecordingSink()

	bundle, _ := sampleBundle(4, 5)
	local, err := r.RouteSignal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ApplyLocal(local, sink) {
		t.Fatal("first ApplyLocal should import the local slice")
	}
	first := len(sink.hosts)
	if r.ApplyLocal(local, sink) {
		t.Fatal("second ApplyLocal at the same epoch should be a no-op")
	}
	if len(sink.hosts) != first {
		t.Fatal("a dropped local slice still imported entries")
	}
}

// TestSignalImportOnCorpus routes a tsumugi import built from the real Common
// Crawl host and URL keys across a fleet and checks the split is lossless and
// owner-exact: every host_score and PageRank entry lands on the one partition
// that owns its host and nowhere else, and the union across partitions is the
// whole bundle. This is the routed-intake path of D16 on real key distributions,
// the thing jump hashing's balance must hold for at scale. It skips when no
// corpus is configured.
func TestSignalImportOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	src := loadCorpusKeys(t, path)
	if len(src.Hosts) < 4 {
		t.Skipf("corpus has %d hosts, need at least 4 to spread an import", len(src.Hosts))
	}

	var bundle m.Signal
	bundle.Epoch = 11
	for i, h := range src.Hosts {
		bundle.Hosts = append(bundle.Hosts, m.HostSignal{HostKey: h.HostKey, HostScore: float32(i%97) / 97})
	}
	for i, u := range src.URLs {
		bundle.URLs = append(bundle.URLs, m.URLSignal{URLKey: u.URLKey, PageRank: float32(i)})
	}

	for _, n := range []int{4, 16, 64} {
		mp := &Map{Epoch: uint64(n), NumPartitions: n}
		tr := NewChannelSignalTransport(len(src.Hosts) + len(src.URLs) + 1)
		producer := NewSignalRouter(0, mp, tr)
		local, err := producer.RouteSignal(bundle)
		if err != nil {
			t.Fatalf("route over %d partitions: %v", n, err)
		}

		sinks := make([]*recordingSink, n)
		for p := range n {
			sinks[p] = newRecordingSink()
		}
		producer.ApplyLocal(local, sinks[0])
		for p := 1; p < n; p++ {
			NewSignalRouter(PartitionID(p), mp, tr).Apply(sinks[p])
		}

		hosts, urls := 0, 0
		for _, h := range bundle.Hosts {
			owner := mp.Owner(h.HostKey)
			if _, ok := sinks[owner].hosts[h.HostKey]; !ok {
				t.Fatalf("n=%d host %d missing from its owner %d", n, h.HostKey, owner)
			}
			for p := range n {
				if PartitionID(p) != owner {
					if _, leaked := sinks[p].hosts[h.HostKey]; leaked {
						t.Fatalf("n=%d host %d leaked to non-owner %d", n, h.HostKey, p)
					}
				}
			}
			hosts++
		}
		for _, u := range bundle.URLs {
			owner := mp.Owner(u.URLKey.HostKey)
			if _, ok := sinks[owner].urls[u.URLKey]; !ok {
				t.Fatalf("n=%d url %v missing from its owner %d", n, u.URLKey, owner)
			}
			urls++
		}
		if hosts != len(bundle.Hosts) || urls != len(bundle.URLs) {
			t.Fatalf("n=%d imported %d/%d hosts, %d/%d urls", n, hosts, len(bundle.Hosts), urls, len(bundle.URLs))
		}
		t.Logf("import over %d partitions: %d hosts, %d urls routed owner-exact and lossless", n, hosts, urls)
	}
}

// TestSendSignalEmptyNoop checks an empty bundle never queues a message, so a
// partition with nothing to import for a destination sends nothing.
func TestSendSignalEmptyNoop(t *testing.T) {
	tr := NewChannelSignalTransport(2)
	if err := tr.SendSignal(0, m.Signal{Epoch: 9}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.RecvSignal(0); ok {
		t.Fatal("empty bundle should not queue a message")
	}
}
