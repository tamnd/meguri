package dedup

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/tamnd/meguri"
)

// cdxRecord is one Common Crawl capture as ccrawl-cli emits it with `-o jsonl`.
// Only the URL is needed here: the seen-set works on canonical keys, not records.
type cdxRecord struct {
	URL string `json:"url"`
}

// corpusPath returns the corpus path or "" when no corpus is configured, so the
// gate skips cleanly on a machine that has not pulled the slice.
func corpusPath() string { return os.Getenv("MEGURI_CORPUS") }

// loadCorpusKeys reads the frozen ccrawl slice and canonicalizes every URL into
// a 128-bit URLKey through the real discovery path (CanonicalKey under the default
// registrable-domain grouping). The returned slice is the exact stream the M3
// discovery loop will hand the seen-set, so the gate exercises canon + key + dedup
// end to end on real Common Crawl data, not synthetic keys.
func loadCorpusKeys(tb testing.TB, path string) []meguri.URLKey {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	var keys []meguri.URLKey
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec cdxRecord
		if json.Unmarshal([]byte(line), &rec) != nil || rec.URL == "" {
			continue
		}
		// No base: the corpus carries absolute URLs. Default grouping and policy
		// are exactly what the frontier wires in New.
		key, _, _, ok := CanonicalKey(rec.URL, "", meguri.GroupRegistrableDomain, nil)
		if !ok {
			continue
		}
		keys = append(keys, key)
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return keys
}

// loadCorpusURLs reads the frozen ccrawl slice and returns the canonical URL text
// of every record, the input the trap detector reads (it parses paths and queries,
// not just keys). It runs the same canonicalization the discovery path does, so the
// detector sees exactly the URLs the frontier would admit.
func loadCorpusURLs(tb testing.TB, path string) []string {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	var urls []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec cdxRecord
		if json.Unmarshal([]byte(line), &rec) != nil || rec.URL == "" {
			continue
		}
		canon, ok := Canonicalize(rec.URL, "", nil)
		if !ok {
			continue
		}
		urls = append(urls, canon)
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return urls
}

// TestCorpusSeenSetZeroFalseNegatives is the M2 gate on real data: feed every
// canonical key from the frozen CC-MAIN-2026-25 slice through the seen-set in
// stream order and require it to agree with a brute-force map oracle on every
// single key. A false negative (a genuinely-seen key called new) would silently
// re-admit a URL and is the one error the tiered filter must never make. It also
// reports the dedup rate and resident cost per URL, the numbers that decide
// whether the design holds at 100B pages.
func TestCorpusSeenSetZeroFalseNegatives(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(t, path)
	if len(keys) == 0 {
		t.Fatalf("corpus %s produced no canonical keys", path)
	}

	s := NewSeenSet(WithCapacity(uint64(len(keys))))
	oracle := make(map[meguri.URLKey]struct{}, len(keys))

	for i, k := range keys {
		_, known := oracle[k]
		got := s.Seen(k)
		if known && !got {
			t.Fatalf("false negative at stream position %d: oracle saw this key, seen-set called it new", i)
		}
		if !known && got {
			// The exact tier forbids this: a never-before key reported seen would
			// be a false positive of the whole set, not just the filter.
			t.Fatalf("false positive at stream position %d: unseen key reported seen", i)
		}
		oracle[k] = struct{}{}
	}

	distinct := s.Len()
	if distinct != len(oracle) {
		t.Fatalf("seen-set holds %d distinct keys, oracle holds %d", distinct, len(oracle))
	}
	dupRate := 1 - float64(distinct)/float64(len(keys))
	t.Logf("corpus: %d urls, %d distinct (%.1f%% duplicate keys), %.2f resident bits/url",
		len(keys), distinct, dupRate*100, s.BitsPerURL())
}

// TestCorpusMergeMatchesStream checks the batched DRUM path against the single-key
// path on real data: classifying the whole corpus as one batch must yield exactly
// the same unique set as inserting every key one at a time. This is the guarantee
// the storage milestone leans on when it swaps per-URL confirms for bucket-sorted
// sequential merges.
func TestCorpusMergeMatchesStream(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	keys := loadCorpusKeys(t, path)
	if len(keys) == 0 {
		t.Fatalf("corpus %s produced no canonical keys", path)
	}

	batched := NewSeenSet(WithCapacity(uint64(len(keys))))
	uniqueBatched := 0
	for _, c := range batched.Merge(keys) {
		if c.Unique {
			uniqueBatched++
		}
	}

	stream := NewSeenSet(WithCapacity(uint64(len(keys))))
	uniqueStream := 0
	for _, k := range keys {
		if !stream.Seen(k) {
			uniqueStream++
		}
	}

	if uniqueBatched != uniqueStream {
		t.Fatalf("DRUM merge found %d uniques, stream found %d", uniqueBatched, uniqueStream)
	}
	if batched.Len() != stream.Len() {
		t.Fatalf("Len mismatch: merge %d, stream %d", batched.Len(), stream.Len())
	}
	t.Logf("corpus: %d urls collapse to %d uniques on both paths", len(keys), uniqueBatched)
}
