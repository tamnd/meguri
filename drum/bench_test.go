package drum

import (
	"fmt"
	"testing"

	"github.com/tamnd/meguri"
)

// scatter spreads i across the 128-bit keyspace so keys land in all 256 buckets
// and sort order is non-trivial, the way real HostKeys (xxHash64) do.
func scatter(i uint64) meguri.URLKey {
	h := i*0x9E3779B97F4A7C15 + 0x123456789
	p := i*0xC2B2AE3D27D4EB4F + 0x9E3779B1
	return meguri.URLKey{HostKey: h, PathKey: p}
}

// BenchmarkMerge folds a batch of fresh discoveries into a pre-built repository and
// reports the per-discovery merge cost: the spec's central throughput claim (doc 04
// section 5), measured rather than modeled. Each op is one full merge cycle over the
// whole repository, so b.N controls the number of merges, and the reported
// discoveries/s is batch / per-op-time.
func BenchmarkMerge(b *testing.B) {
	for _, batch := range []int{100000, 500000} {
		b.Run(fmt.Sprintf("batch-%d", batch), func(b *testing.B) {
			dir := b.TempDir()
			d, err := Open(dir, Options{FlushBytes: 8 << 20, ReadBuf: 4 << 20, WriteBuf: 4 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer d.Close()
			// Seed a repository so the merge sweeps a real file each cycle.
			var seq uint64 = 1
			for i := 0; i < 1000000; i++ {
				if err := d.Discover(scatter(seq), int64(seq*32), seq); err != nil {
					b.Fatal(err)
				}
				seq++
			}
			if _, err := d.Merge(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for i := 0; i < batch; i++ {
					if err := d.Discover(scatter(seq), int64(seq*32), seq); err != nil {
						b.Fatal(err)
					}
					seq++
				}
				if _, err := d.Merge(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			perOp := b.Elapsed().Seconds() / float64(b.N)
			b.ReportMetric(float64(batch)/perOp, "disc/s")
		})
	}
}

// BenchmarkLocate measures the cold point read: one resident block-index binary
// search plus one block ReadAt (doc 04 section 4.2). The keys probed are spread
// across the whole repository so the block reads do not all hit one cached block.
func BenchmarkLocate(b *testing.B) {
	dir := b.TempDir()
	d, err := Open(dir, Options{FlushBytes: 8 << 20, ReadBuf: 4 << 20, WriteBuf: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	const n = 2000000
	for i := uint64(1); i <= n; i++ {
		if err := d.Discover(scatter(i), int64(i*32), i); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := d.Merge(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := scatter(uint64(i%n) + 1)
		if _, _, present, err := d.Locate(k); err != nil || !present {
			b.Fatalf("locate miss: present=%v err=%v", present, err)
		}
	}
}
