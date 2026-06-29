package frontier

import (
	"context"
	"os"
	"testing"

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
