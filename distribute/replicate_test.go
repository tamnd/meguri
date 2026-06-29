package distribute

import (
	"bytes"
	"os"
	"sort"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

func mustEncode(tb testing.TB, p *format.Partition) []byte {
	tb.Helper()
	b, err := format.Encode(p)
	if err != nil {
		tb.Fatalf("encode partition: %v", err)
	}
	return b
}

// applyTo folds the same update a tail entry carries directly into a reference
// map, the authoritative state a replica must reconstruct. It is deliberately not
// the Replica.Apply path, so the corpus gate compares the replication code under
// test against an independent application of the same updates.
func applyTo(urls map[m.URLKey]m.URLRecord, e TailEntry) {
	switch e.Kind {
	case TailPutURL:
		urls[e.URL.URLKey] = e.URL
	case TailDelURL:
		delete(urls, e.URLKey)
	}
}

// partitionFrom builds a sorted format.Partition from a URL map, matching what
// Replica.Partition produces, so two partitions with the same logical content
// encode to the same bytes.
func partitionFrom(id uint32, created uint32, codec uint8, urls map[m.URLKey]m.URLRecord, hosts []m.HostRecord) *format.Partition {
	out := make([]m.URLRecord, 0, len(urls))
	for _, u := range urls {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URLKey.Less(out[j].URLKey) })
	hs := append([]m.HostRecord(nil), hosts...)
	sort.Slice(hs, func(i, j int) bool { return hs[i].HostKey < hs[j].HostKey })
	lo, hi := uint64(0), ^uint64(0)
	if len(hs) > 0 {
		lo, hi = hs[0].HostKey, hs[len(hs)-1].HostKey
	}
	return &format.Partition{
		ID:           id,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: created,
		DefaultCodec: codec,
		URLs:         out,
		Hosts:        hs,
		Strings:      nil,
	}
}

func TestReplicaAppliesTailInOrder(t *testing.T) {
	k1 := m.MakeURLKey("a.example", "/1")
	k2 := m.MakeURLKey("a.example", "/2")
	snap := &format.Partition{
		ID:    7,
		URLs:  []m.URLRecord{{URLKey: k1, HostKey: k1.HostKey, Status: m.StatusCrawled}},
		Hosts: []m.HostRecord{{HostKey: k1.HostKey, Grouping: m.GroupFullHost}},
	}
	r := NewReplica(snap, 100)
	if r.AppliedLSN() != 100 {
		t.Fatalf("applied = %d, want the snapshot LSN 100", r.AppliedLSN())
	}

	// A write past the snapshot, then a write to a new key, then a tombstone.
	r.Stream([]TailEntry{
		PutURL(101, m.URLRecord{URLKey: k1, HostKey: k1.HostKey, Status: m.StatusDueRecrawl}),
		PutURL(102, m.URLRecord{URLKey: k2, HostKey: k2.HostKey, Status: m.StatusScheduled}),
		DelURL(103, k1),
	})
	if r.AppliedLSN() != 103 {
		t.Fatalf("applied = %d, want 103", r.AppliedLSN())
	}
	p := r.Partition()
	if len(p.URLs) != 1 || p.URLs[0].URLKey != k2 {
		t.Fatalf("after a delete of k1 the replica should hold only k2, got %d urls", len(p.URLs))
	}
}

func TestReplicaIgnoresStaleRedelivery(t *testing.T) {
	k := m.MakeURLKey("a.example", "/1")
	snap := &format.Partition{URLs: []m.URLRecord{{URLKey: k, HostKey: k.HostKey, Status: m.StatusScheduled}}}
	r := NewReplica(snap, 10)

	r.Apply(PutURL(12, m.URLRecord{URLKey: k, HostKey: k.HostKey, Status: m.StatusCrawled}))
	// A redelivery at or below the applied LSN must not roll the state back.
	r.Apply(PutURL(12, m.URLRecord{URLKey: k, HostKey: k.HostKey, Status: m.StatusScheduled}))
	r.Apply(PutURL(5, m.URLRecord{URLKey: k, HostKey: k.HostKey, Status: m.StatusDiscovered}))

	p := r.Partition()
	if p.URLs[0].Status != m.StatusCrawled {
		t.Fatalf("stale redelivery rolled the state back to %v, want Crawled", p.URLs[0].Status)
	}
	if r.AppliedLSN() != 12 {
		t.Fatalf("applied = %d, want 12", r.AppliedLSN())
	}
}

func TestReplicaLag(t *testing.T) {
	snap := &format.Partition{}
	r := NewReplica(snap, 0)
	r.Apply(PutURL(40, m.URLRecord{URLKey: m.MakeURLKey("a.example", "/x")}))
	if got := r.Lag(100); got != 60 {
		t.Fatalf("lag = %d, want 60", got)
	}
	if got := r.Lag(30); got != 0 {
		t.Fatalf("lag behind a smaller primary LSN = %d, want 0", got)
	}
}

func TestRecoverInFlightResetsToScheduled(t *testing.T) {
	p := &format.Partition{URLs: []m.URLRecord{
		{Status: m.StatusInFlight},
		{Status: m.StatusCrawled},
		{Status: m.StatusInFlight},
		{Status: m.StatusScheduled},
	}}
	if n := RecoverInFlight(p); n != 2 {
		t.Fatalf("reset %d, want 2", n)
	}
	for _, u := range p.URLs {
		if u.Status == m.StatusInFlight {
			t.Fatal("an InFlight URL survived recovery")
		}
	}
}

// TestReplicateOnCorpus is the M9 replication gate on real data: load the frozen
// ccrawl slice as a primary partition, snapshot it, stream a log tail of real
// record updates onto a replica, and require the replica to be byte-for-byte the
// partition the primary became, idempotent under redelivery. This is the spec's
// claim that a replica is a partition recovered up to the tail it has received
// (doc 12, section 4), checked against Common Crawl's real key distribution.
func TestReplicateOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	src := loadCorpusKeys(t, path)
	if len(src.URLs) < 100 {
		t.Skipf("corpus has %d urls, need at least 100 to build a meaningful tail", len(src.URLs))
	}

	const baseLSN = 1000
	// The reference state starts as the snapshot's URLs and folds the same tail
	// the replica will, by a path that is not Replica.Apply.
	want := make(map[m.URLKey]m.URLRecord, len(src.URLs))
	for _, u := range src.URLs {
		want[u.URLKey] = u
	}

	// Build a tail over the first 64 urls: recrawl most, mark a few in flight,
	// and tombstone the last two as if their hosts left the partition.
	var tail []TailEntry
	k := 64
	lsn := uint64(baseLSN)
	inFlight := 0
	for i := range k {
		lsn++
		rec := src.URLs[i]
		switch {
		case i >= k-2:
			tail = append(tail, DelURL(lsn, rec.URLKey))
			applyTo(want, DelURL(lsn, rec.URLKey))
			continue
		case i%7 == 0:
			rec.Status = m.StatusInFlight
			inFlight++
		default:
			rec.Status = m.StatusDueRecrawl
			rec.HTTPStatus = 304
			rec.CrawlCount++
		}
		tail = append(tail, PutURL(lsn, rec))
		applyTo(want, PutURL(lsn, rec))
	}

	replica := NewReplica(src, baseLSN)
	replica.Stream(tail)

	wantPart := partitionFrom(src.ID, src.CreatedHours, src.DefaultCodec, want, src.Hosts)
	gotBytes := mustEncode(t, replica.Partition())
	wantBytes := mustEncode(t, wantPart)
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("replica state diverged from the primary: replica %d bytes, primary %d bytes", len(gotBytes), len(wantBytes))
	}

	// Redelivering the whole tail must not change a thing.
	replica.Stream(tail)
	if again := mustEncode(t, replica.Partition()); !bytes.Equal(again, wantBytes) {
		t.Fatal("redelivering the tail changed the replica state")
	}

	// The materialized partition must still round-trip through the on-disk format.
	if _, err := format.Decode(gotBytes); err != nil {
		t.Fatalf("promoted replica is not a well-formed .meguri file: %v", err)
	}

	t.Logf("replicated %d urls + %d tail entries (%d in flight, 2 tombstoned), replica byte-identical to primary at LSN %d",
		len(src.URLs), len(tail), inFlight, lsn)
}
