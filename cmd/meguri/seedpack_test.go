package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	meguri "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/seed"
)

// writeCorpus writes a JSONL corpus of the given URLs to a temp file and returns its
// path.
func writeCorpus(t *testing.T, dir string, urls []string) string {
	t.Helper()
	var b bytes.Buffer
	for _, u := range urls {
		fmt.Fprintf(&b, "{\"url\":%q}\n", u)
	}
	path := filepath.Join(dir, "corpus.jsonl")
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// makeCorpusURLs builds a set of URLs spread across many hosts, enough that a uniform
// hostkey hash lands URLs in every shard.
func makeCorpusURLs(hosts, perHost int) []string {
	urls := make([]string, 0, hosts*perHost)
	for h := 0; h < hosts; h++ {
		for p := 0; p < perHost; p++ {
			urls = append(urls, fmt.Sprintf("http://host%04d.example.com/page/%d", h, p))
		}
	}
	return urls
}

// TestSeedpackRoutesAndRoundTrips packs a corpus into shards and checks that every URL
// lands in the shard its hostkey routes to, that the ranges tile the space, that the
// total is preserved, and that each shard's URLs read back intact.
func TestSeedpackRoutesAndRoundTrips(t *testing.T) {
	for _, codec := range []seed.Codec{seed.CodecRaw, seed.CodecZstd} {
		for _, shards := range []int{1, 4, 8, 32} {
			t.Run(fmt.Sprintf("codec=%d/shards=%d", codec, shards), func(t *testing.T) {
				dir := t.TempDir()
				urls := makeCorpusURLs(200, 5)
				corpus := writeCorpus(t, dir, urls)
				out := filepath.Join(dir, "shards")
				if err := os.MkdirAll(out, 0o755); err != nil {
					t.Fatal(err)
				}

				var log bytes.Buffer
				if err := runSeedpack(&log, corpus, out, shards, seed.DefaultBlockSize, codec); err != nil {
					t.Fatalf("runSeedpack: %v", err)
				}

				man, err := seed.ReadManifest(out)
				if err != nil {
					t.Fatalf("ReadManifest: %v", err)
				}
				if int(man.Records) != len(urls) {
					t.Fatalf("manifest records = %d, want %d", man.Records, len(urls))
				}

				// Ranges must tile [0, max] with no gap or overlap, ascending.
				if man.Shards[0].HostLo != 0 {
					t.Fatalf("first shard HostLo = %d, want 0", man.Shards[0].HostLo)
				}
				if last := man.Shards[len(man.Shards)-1].HostHi; last != ^uint64(0) {
					t.Fatalf("last shard HostHi = %d, want max", last)
				}
				for i := 1; i < len(man.Shards); i++ {
					if man.Shards[i].HostLo != man.Shards[i-1].HostHi {
						t.Fatalf("gap between shard %d and %d", i-1, i)
					}
				}

				// Every URL read back from every shard must fall in that shard's range,
				// and the union must equal the input set with counts preserved.
				seen := map[string]int{}
				var readTotal uint64
				for _, sm := range man.Shards {
					r, err := seed.Open(filepath.Join(out, sm.Path))
					if err != nil {
						t.Fatalf("open shard %d: %v", sm.Index, err)
					}
					lo, hi := r.HostRange()
					if lo != sm.HostLo || hi != sm.HostHi {
						t.Fatalf("shard %d header range [%d,%d) != manifest [%d,%d)", sm.Index, lo, hi, sm.HostLo, sm.HostHi)
					}
					var shardCount uint64
					for bi := 0; bi < r.Blocks(); bi++ {
						br, err := r.BlockReader(bi)
						if err != nil {
							t.Fatalf("block %d: %v", bi, err)
						}
						for {
							u, ok := br.Next()
							if !ok {
								break
							}
							url := string(u)
							hk := meguri.HostKeyOf(frontier.HostOf(url))
							if man.Route(hk) != sm.Index {
								t.Fatalf("url %q routes to shard %d but sits in shard %d", url, man.Route(hk), sm.Index)
							}
							if hk < lo || hk >= hi {
								t.Fatalf("url %q hostkey %d outside shard range [%d,%d)", url, hk, lo, hi)
							}
							seen[url]++
							shardCount++
						}
					}
					if shardCount != sm.Records {
						t.Fatalf("shard %d read %d urls, manifest says %d", sm.Index, shardCount, sm.Records)
					}
					readTotal += shardCount
					_ = r.Close()
				}
				if int(readTotal) != len(urls) {
					t.Fatalf("read %d urls total, want %d", readTotal, len(urls))
				}
				for _, u := range urls {
					if seen[u] != 1 {
						t.Fatalf("url %q seen %d times, want 1", u, seen[u])
					}
				}
			})
		}
	}
}
