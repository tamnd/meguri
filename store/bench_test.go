package store

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/tamnd/meguri"
)

// benchRec builds a throwaway URL record for host with path index i.
func benchRec(host string, i int) *meguri.URLRecord {
	return &meguri.URLRecord{
		URLKey:   meguri.MakeURLKey(host, fmt.Sprintf("/p%d", i)),
		HostKey:  meguri.HostKeyOf(host),
		Status:   meguri.StatusDueRecrawl,
		Priority: float32(i),
	}
}

// BenchmarkPutURLNone measures the in-memory write path with no fsync: the
// append-and-repoint cost the log-structured store turns a random update into
// (doc 11 section 1.2). This is the speed ceiling the durability dial trades off.
func BenchmarkPutURLNone(b *testing.B) {
	s, err := Open(b.TempDir(), Options{Durability: DurabilityNone})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	rec := benchRec("go.dev", 0)
	b.ReportAllocs()
	for b.Loop() {
		s.PutURL(rec)
	}
}

// BenchmarkGetResident measures a point read served from the resident index, the
// single hash probe plus a slice copy (doc 11 section 1.1).
func BenchmarkGetResident(b *testing.B) {
	s, err := Open(b.TempDir(), Options{Durability: DurabilityNone})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	rec := benchRec("go.dev", 1)
	s.PutURL(rec)
	b.ReportAllocs()
	for b.Loop() {
		s.GetURL(rec.URLKey)
	}
}

// BenchmarkGetSpilled measures the larger-than-memory read: a record evicted to
// disk re-materialized with one ReadAt and a decode (doc 11 section 6.2). The
// tiny resident budget forces every read down the spilled path.
func BenchmarkGetSpilled(b *testing.B) {
	s, err := Open(b.TempDir(), Options{Durability: DurabilityNone, ResidentBudget: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	const n = 4096
	keys := make([]meguri.URLKey, n)
	for i := range n {
		rec := benchRec("go.dev", i)
		keys[i] = rec.URLKey
		s.PutURL(rec)
	}
	b.ReportAllocs()
	var i int
	for b.Loop() {
		s.GetURL(keys[i&(n-1)])
		i++
	}
}

// BenchmarkPutFullSerial is the honest conc-1 fsync floor (D19, doc 11 section
// 8): under DurabilityFull a single writer pays one device flush per update, and
// no honest durable store beats that. The benchmark names the floor; the win
// shows in the parallel case below.
func BenchmarkPutFullSerial(b *testing.B) {
	s, err := Open(b.TempDir(), Options{Durability: DurabilityFull})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	rec := benchRec("go.dev", 0)
	b.ReportAllocs()
	for b.Loop() {
		s.PutURL(rec)
	}
}

// BenchmarkPutFullParallel shows the group-commit amortization above conc-1: many
// concurrent crawl updates coalesce onto shared fsyncs, so the per-update device
// cost falls below the conc-1 floor (doc 11 section 8). A partition processing
// many in-flight outcomes is naturally this multi-writer workload.
func BenchmarkPutFullParallel(b *testing.B) {
	s, err := Open(b.TempDir(), Options{Durability: DurabilityFull})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	var ctr atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		host := fmt.Sprintf("h%d.test", ctr.Add(1))
		rec := benchRec(host, 0)
		for pb.Next() {
			s.PutURL(rec)
		}
	})
}

// BenchmarkCheckpoint measures folding a populated store into a .meguri snapshot
// plus the log rotation and superblock commit (doc 11 section 4).
func BenchmarkCheckpoint(b *testing.B) {
	s, err := Open(b.TempDir(), Options{Durability: DurabilityNone})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	for i := range 10000 {
		s.PutURL(benchRec(fmt.Sprintf("h%d.test", i%50), i))
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := s.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
}
