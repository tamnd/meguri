package engine

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/fetch"
	"github.com/tamnd/meguri/frontier"
)

// countFetcher returns a 200 for every request and counts the calls, the minimal
// fetcher the batch and seed-source adapters exercise.
type countFetcher struct{ n atomic.Int64 }

func (f *countFetcher) Fetch(_ context.Context, req fetch.Request) (meguri.Outcome, error) {
	f.n.Add(1)
	return meguri.Outcome{URLKey: req.URLKey, HTTPStatus: 200}, nil
}

// TestBatchFetcherFetchBatch checks the doc-13 batch surface returns one outcome
// per request through the worker pool, in completion order, channel closed at the
// end.
func TestBatchFetcherFetchBatch(t *testing.T) {
	cf := &countFetcher{}
	bf := NewBatchFetcher(cf, 4)
	var reqs []fetch.Request
	for i := range 20 {
		reqs = append(reqs, fetch.Request{URLKey: meguri.URLKey{PathKey: uint64(i)}})
	}
	got := 0
	for range bf.FetchBatch(context.Background(), reqs) {
		got++
	}
	if got != 20 {
		t.Fatalf("got %d outcomes, want 20", got)
	}
	if cf.n.Load() != 20 {
		t.Fatalf("fetcher called %d times, want 20", cf.n.Load())
	}
}

// TestSeedReader converts seeds to discoveries, keying them the same way a crawled
// out-link is keyed, and ingests them into a frontier with their prior validators
// warmed onto the new records.
func TestSeedReader(t *testing.T) {
	r := NewSeedReader()
	d, ok := r.Discovery(Seed{URL: "https://Example.COM/a/../b?utm_source=x", Priority: 0.7})
	if !ok {
		t.Fatal("seed did not convert")
	}
	if d.CanonicalURL != "https://example.com/b" {
		t.Fatalf("canonical url = %q, want https://example.com/b", d.CanonicalURL)
	}
	if d.LinkWeight != 0.7 {
		t.Fatalf("link weight = %v, want 0.7 (priority carried)", d.LinkWeight)
	}

	fr := frontier.New(1, 0)
	seeds := []Seed{
		{URL: "https://a.example/p", Priority: 0.5, PrevETag: "\"v1\"", PrevLastModified: 100},
		{URL: "https://a.example/p", Priority: 0.5}, // duplicate, dedups
		{URL: "not a url", Priority: 0.5},           // dropped
	}
	if added := r.Ingest(fr, seeds, 0); added != 1 {
		t.Fatalf("ingested %d new urls, want 1", added)
	}
	if fr.Len() != 1 {
		t.Fatalf("frontier holds %d urls, want 1", fr.Len())
	}
}

// TestMeguriSeedSource drives the inverse adapter: a puller calls Next until the
// frontier drains, fetching each request and handing the outcome back through
// Report, so the partition presents the ami.SeedSource shape correctly.
func TestMeguriSeedSource(t *testing.T) {
	fr := frontier.New(1, 0)
	for _, h := range []string{"a.example", "b.example"} {
		for i := range 3 {
			fr.Seed("https://"+h+"/p/"+string(rune('a'+i)), h, 0.5, 0, 0, 10)
		}
	}
	clk := NewLogicalClock(500_000)
	src := NewSeedSource(fr, clk)
	cf := &countFetcher{}

	pulled := 0
	for {
		req, ok := src.Next(context.Background())
		if !ok {
			break
		}
		o, _ := cf.Fetch(context.Background(), req)
		src.Report(o)
		pulled++
		if pulled > 100 {
			t.Fatal("seed source did not drain")
		}
	}
	if pulled != 6 {
		t.Fatalf("pulled %d urls, want 6", pulled)
	}
	if fr.Pending() != 0 {
		t.Fatalf("pending %d after drain, want 0", fr.Pending())
	}
}
