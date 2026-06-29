package frontier

import (
	"context"
	"os"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// BenchmarkSeed measures the cost of building a frontier from scratch: interning
// the URL, deriving the 128-bit key, and pushing it into the front bank.
func BenchmarkSeed(b *testing.B) {
	seeds := syntheticSeeds(200, 50, 20) // 10k urls across 200 hosts
	b.ReportAllocs()
	for b.Loop() {
		f := New(1, 0)
		for _, s := range seeds {
			f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
		}
	}
}

// BenchmarkDrain measures the full scheduler loop: dispatch every URL in
// priority-then-politeness order, advancing the clock over politeness waits, and
// report each outcome. This is the hot path of the engine.
func BenchmarkDrain(b *testing.B) {
	seeds := syntheticSeeds(200, 50, 20)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		f := seedAll(New(1, 0), seeds)
		b.StartTimer()
		if _, err := f.Drain(context.Background(), 0, stubFetcher{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCheckpoint measures serializing a live frontier to the on-disk
// .meguri image, the cost a periodic checkpoint pays.
func BenchmarkCheckpoint(b *testing.B) {
	f := seedAll(New(1, 0), syntheticSeeds(200, 50, 20))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.CheckpointBytes(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecover measures rebuilding a scheduler from a checkpoint image, the
// cost a restart pays.
func BenchmarkRecover(b *testing.B) {
	f := seedAll(New(1, 0), syntheticSeeds(200, 50, 20))
	raw, err := f.CheckpointBytes()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for b.Loop() {
		p, err := format.Decode(raw)
		if err != nil {
			b.Fatal(err)
		}
		_ = Recover(p)
	}
}

// BenchmarkCorpusDrain runs the scheduler over the frozen ccrawl slice, so the
// dispatch hot path is measured on real URLs and real hosts at corpus scale. It
// skips when no corpus is configured.
func BenchmarkCorpusDrain(b *testing.B) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	seeds := loadCorpusSeeds(b, path)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		f := seedAll(New(1, 0), seeds)
		b.StartTimer()
		if _, err := f.Drain(context.Background(), 0, stubFetcher{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCorpusDrainPolite runs the full M3 hot path over the real slice: DNS
// resolution off the dispatch path, the per-host and shared per-IP politeness
// buckets, and AIMD folding each replayed status. It is the cost the scheduler
// pays per fetch once politeness is on, the number that has to hold at 100B
// pages. It skips when no corpus is configured.
func BenchmarkCorpusDrainPolite(b *testing.B) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	seeds := loadCorpusSeeds(b, path)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		f := New(1, 0, WithResolver(poolResolver{pool: 8}))
		status := map[meguri.URLKey]uint16{}
		for _, s := range seeds {
			f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
			status[meguri.MakeURLKey(s.host, PathOf(s.url))] = s.status
		}
		f.resolver.Wait()
		b.StartTimer()
		if _, err := f.Drain(context.Background(), 0, &scriptFetcher{status: status}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCorpusDispatchSelections isolates the scheduler's own selection rate
// on the real slice and reports it as selections/s, the doc 14 section 5.3
// headline. Drain advances its simulated clock over every politeness wait for
// free, so the wall-clock time it takes is the cost of the selection machinery
// alone (pop the best ready host, resolve, re-check the live window, pick the
// head URL, fold the outcome), not of any politeness delay. The reported sel/s is
// the rate a single dispatch loop sustains before any fetcher or politeness floor
// enters, the number the throughput analysis measures the politeness ceiling
// against. It skips when no corpus is configured.
func BenchmarkCorpusDispatchSelections(b *testing.B) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	seeds := loadCorpusSeeds(b, path)
	var total int
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		f := seedAll(New(1, 0), seeds)
		b.StartTimer()
		d, err := f.Drain(context.Background(), 0, stubFetcher{})
		if err != nil {
			b.Fatal(err)
		}
		total += len(d)
	}
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "sel/s")
}

// BenchmarkCorpusCheckpoint measures a checkpoint of the full corpus frontier.
func BenchmarkCorpusCheckpoint(b *testing.B) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	f := seedAll(New(1, 0), loadCorpusSeeds(b, path))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.CheckpointBytes(); err != nil {
			b.Fatal(err)
		}
	}
}
