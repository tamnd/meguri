package dedup

import (
	"strings"
	"testing"

	"github.com/tamnd/meguri"
)

// BenchmarkSeenSet measures the steady-state check-and-insert: the per-URL cost
// the discovery loop pays for every link it pulls off a page. The key space is a
// third the size of the stream so two thirds of the calls hit the duplicate path,
// the realistic mix the corpus shows.
func BenchmarkSeenSet(b *testing.B) {
	const space = 1 << 18
	s := NewSeenSet(WithCapacity(space))
	keys := make([]meguri.URLKey, space)
	for i := range keys {
		keys[i] = keyN(i)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		s.Seen(keys[i%space])
		i++
	}
}

// BenchmarkCanonicalize measures the cost of turning a raw link into a canonical
// URL, the per-link work that runs before the seen-set ever sees a key.
func BenchmarkCanonicalize(b *testing.B) {
	const raw = "HTTP://Example.COM:80/a/./b/../c/index.html?utm_source=x&id=42#frag"
	b.ReportAllocs()
	for b.Loop() {
		Canonicalize(raw, "", nil)
	}
}

// BenchmarkCanonicalKey measures the full discovery-path call: canonicalize and
// derive the 128-bit key in one shot, which is what the frontier actually invokes.
func BenchmarkCanonicalKey(b *testing.B) {
	const raw = "https://shop.example.com/products/widget?ref=home&utm_campaign=spring"
	b.ReportAllocs()
	for b.Loop() {
		CanonicalKey(raw, "", meguri.GroupRegistrableDomain, nil)
	}
}

// BenchmarkSimhash measures the near-dup signature over a document-length body,
// the per-fetch cost the freshness join pays to tell a cosmetic change from a real
// one.
func BenchmarkSimhash(b *testing.B) {
	body := strings.Repeat(pageText, 12)
	b.ReportAllocs()
	for b.Loop() {
		SimhashText(body)
	}
}

// BenchmarkContentFP measures the exact body fingerprint, the cheap first check
// that catches a byte-identical refetch before any simhash work.
func BenchmarkContentFP(b *testing.B) {
	body := []byte(strings.Repeat(pageText, 12))
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		ContentFP(body)
	}
}

// BenchmarkCorpusSeenSet measures the real per-URL dedup cost on the frozen
// CC-MAIN-2026-25 slice: stream every canonical key through a fresh seen-set. This
// is the number that projects to 100B pages.
func BenchmarkCorpusSeenSet(b *testing.B) {
	path := corpusPath()
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(b, path)
	b.SetBytes(int64(len(keys)))
	b.ReportAllocs()
	for b.Loop() {
		s := NewSeenSet(WithCapacity(uint64(len(keys))))
		for _, k := range keys {
			s.Seen(k)
		}
	}
}

// BenchmarkCorpusMerge measures the batched DRUM path on real data: the
// bucket-sorted sequential merge the storage milestone uses in place of per-URL
// random confirms.
func BenchmarkCorpusMerge(b *testing.B) {
	path := corpusPath()
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(b, path)
	b.SetBytes(int64(len(keys)))
	b.ReportAllocs()
	for b.Loop() {
		s := NewSeenSet(WithCapacity(uint64(len(keys))))
		s.Merge(keys)
	}
}
