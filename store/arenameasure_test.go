package store

import (
	"bufio"
	"encoding/json"
	"os"
	"runtime"
	"testing"
)

// TestArenaResidencyMeasure measures the spilled-arena residency win (spec 2072
// doc 05, Stage A) on a REAL corpus, the numbers that go into 2071/implementation
// in place of projections. It is skipped unless MEGURI_ARENA_CORPUS points at a
// scale-NM.jsonl profile, so the normal `go test` stays fast and corpus-free.
//
//	MEGURI_ARENA_CORPUS=corpus/profiles/scale-1m.jsonl \
//	MEGURI_ARENA_MIB=256 go test ./store/ -run ArenaResidencyMeasure -v
//
// It builds the canonical-URL arena the way Store.Intern lays it out, then reports
// the held heap two ways: the whole arena resident in a []byte (today), versus the
// arena spilled to a file and read by offset through the bounded arenaCache. The
// resident path's held heap grows ~B/url; the spilled path's is the flat B_arena.
func TestArenaResidencyMeasure(t *testing.T) {
	path := os.Getenv("MEGURI_ARENA_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_ARENA_CORPUS=corpus/profiles/scale-1m.jsonl to run")
	}
	budgetMiB := int64(envInt("MEGURI_ARENA_MIB", 256))
	limit := envInt("MEGURI_ARENA_N", 0)
	reads := envInt("MEGURI_ARENA_READS", 2_000_000)

	urls := loadArenaURLs(t, path, limit)
	if len(urls) == 0 {
		t.Fatalf("no urls loaded from %s", path)
	}
	t.Logf("corpus=%s urls=%d B_arena=%d MiB", path, len(urls), budgetMiB)

	// Lay out the arena exactly as Store.Intern does: offset-0 sentinel, each
	// entry a uvarint length then the bytes, and record each url's offset.
	arena := []byte{0}
	offs := make([]uint64, len(urls))
	for i, u := range urls {
		offs[i] = uint64(len(arena))
		arena = appendUvarint(arena, uint64(len(u)))
		arena = append(arena, u...)
	}
	t.Logf("arena bytes=%d (%.1f B/url, incl uvarint length prefixes)",
		len(arena), float64(len(arena))/float64(len(urls)))

	// Resident cost is exactly the contiguous arena []byte the store holds today:
	// len(arena). Measuring it via held heap would fold in the test's offset slice
	// and slack; the byte length is the unambiguous resident figure.
	residentBytes := int64(len(arena))

	// Spilled path: write the arena to a file, read by offset through the cache.
	tmp, err := os.CreateTemp(t.TempDir(), "arena-*.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write(arena); err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	af, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer af.Close()
	sa := newSpilledArena(af, int64(len(arena)), budgetMiB<<20)

	// Golden gate: every offset resolves byte-identically to the resident read.
	for i, off := range offs {
		if sa.readArenaAt(off) != readArena(arena, off) {
			t.Fatalf("golden mismatch url[%d] off=%d", i, off)
		}
	}
	t.Log("golden: all spilled reads byte-identical to resident reads")

	// Drive a host-burst dispatch pattern (doc 05 section 3d) for the hit rate.
	driveDispatch(sa, offs, reads)

	// Isolate the spilled arena's resident cost: drop both the resident []byte and
	// the test's offset slice so the measured held heap is the cache alone (its
	// node pool, map, and the decoded working-set strings). offs is the store's
	// urlLoc index in production, a separate structure, so it does not belong in
	// the arena's residency number.
	arena = nil
	offs = nil
	runtime.GC()
	spilledHeld := heldHeapWith(sa)

	used, b, hits, misses, evicted := sa.stats()
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = 100 * float64(hits) / float64(total)
	}
	mult := float64(spilledHeld) / float64(b) // real heap per counted budget byte

	t.Logf("resident arena (contiguous []byte the store holds today):")
	t.Logf("  %9.2f MiB  (%5.1f B/url)", mibf(residentBytes), perURL(residentBytes, len(urls)))
	t.Logf("spilled arena (held heap of the cache alone, after GC):")
	t.Logf("  %9.2f MiB  (%5.1f B/url)  cache_used=%d MiB budget=%d MiB real_mult=%.2fx",
		mibf(spilledHeld), perURL(spilledHeld, len(urls)), used>>20, b>>20, mult)
	t.Logf("removed: %9.2f MiB  (%5.1f B/url)",
		mibf(residentBytes-spilledHeld), perURL(residentBytes-spilledHeld, len(urls)))
	t.Logf("dispatch reads=%d hits=%d misses=%d hit_rate=%.2f%% evicted=%d", total, hits, misses, hitRate, evicted)

	residPerURL := float64(residentBytes) / float64(len(urls))
	t.Logf("projected at 100M urls: resident arena %.2f GiB  ->  spilled arena %.2f GiB (flat, real_mult applied)",
		residPerURL*1e8/(1<<30), float64(spilledHeld)/(1<<30))
}

// driveDispatch reads urls in a host-burst-locality pattern: a sliding working
// set read repeatedly, interleaved with a sparse cold tail (freshness revisits),
// the access shape doc 05 section 3d says a small LRU serves well.
func driveDispatch(sa *spilledArena, offs []uint64, reads int) {
	n := len(offs)
	if n == 0 {
		return
	}
	const window = 50000
	for i := range reads {
		var idx int
		if i%8 == 0 {
			idx = (i * 2654435761) % n // cold tail
		} else {
			idx = ((i/4)%n + i%window) % n // advancing dispatch front
		}
		_ = sa.readArenaAt(offs[idx])
	}
}

func heldHeapWith(keep any) int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	runtime.KeepAlive(keep)
	return int64(m.HeapInuse)
}

func mibf(b int64) float64 { return float64(b) / (1 << 20) }
func perURL(b int64, n int) float64 {
	if n == 0 {
		return 0
	}
	return float64(b) / float64(n)
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func loadArenaURLs(t *testing.T, path string, limit int) []string {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var urls []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	var rec struct {
		URL string `json:"url"`
	}
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if json.Unmarshal(line, &rec) != nil || rec.URL == "" {
			continue
		}
		urls = append(urls, rec.URL)
		if limit > 0 && len(urls) >= limit {
			break
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return urls
}
