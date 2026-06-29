package store

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/meguri"
)

// This file is the M6 gate on real data (doc 11, doc 14): the durable store is
// loaded from the frozen ccrawl slice (CC-MAIN-2026-25, the tamnd/ccrawl-cli
// corpus pinned in corpus/), checkpointed into a .meguri file, recovered, and
// checked record-for-record. Two real-data properties carry the gate.
//
// First, checkpoint-and-recover identity on a real partition. Every distinct URL
// in the slice becomes a URLRecord with its real canonical URL interned, its real
// HTTP status, and a host record per host group. A checkpoint folds the live
// store into a .meguri snapshot, a recovery rebuilds the index and the string
// arena from that snapshot, and every record plus its URL string must survive
// byte-for-byte. This exercises the section 4 checkpoint and the section 5 redo
// recovery on a real frontier slice, and reports the snapshot's bytes-per-URL.
//
// Second, larger-than-memory on real records. Loading the slice under a small
// resident budget forces the hybrid-log spill: cold record bodies evict to the
// log and re-materialize from disk on read (section 6). The gate checks the
// resident set stays bounded while every real URL stays reachable and reads back
// with its real fields.
//
// The honesty flag (D19, doc 11 section 8): the conc-1 fully-synced write sits on
// the device fsync floor, named not hidden. The store benchmarks report it
// (BenchmarkPutFullSerial) alongside the multi-writer group-commit cell where the
// amortization shows; this gate runs the durable path end to end on real records
// to confirm correctness, not to claim a throughput number the doc 14 hardware
// gate has not yet measured.

// corpusURL is one distinct URL loaded from the slice, with the fields the store
// gate stamps into its record.
type corpusURL struct {
	url    string
	host   string
	path   string
	status uint16
}

// loadCorpusURLs reads the pinned ccrawl jsonl slice and returns the distinct
// URLs, deduplicated by URLKey so the count matches what the store holds.
func loadCorpusURLs(tb testing.TB, path string, limit int) []corpusURL {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type row struct {
		URL    string `json:"url"`
		Status string `json:"status"`
	}
	seen := map[meguri.URLKey]struct{}{}
	var out []corpusURL
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r row
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		host := corpusHost(r.URL)
		if host == "" {
			continue
		}
		path := corpusPath(r.URL)
		key := meguri.MakeURLKey(host, path)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, corpusURL{url: r.URL, host: host, path: path, status: parseStatus(r.Status)})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return out
}

// loadStore opens a fresh store and fills it with the corpus URLs and a host
// record per host group, returning the store and the loaded URLs.
func loadStore(tb testing.TB, dir string, opts Options, urls []corpusURL) *Store {
	tb.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	hostsSeen := map[uint64]struct{}{}
	for i := range urls {
		u := &urls[i]
		ref, err := s.Intern(u.url)
		if err != nil {
			tb.Fatalf("intern: %v", err)
		}
		hk := meguri.HostKeyOf(u.host)
		if _, err := s.PutURL(&meguri.URLRecord{
			URLKey:          meguri.MakeURLKey(u.host, u.path),
			HostKey:         hk,
			Status:          meguri.StatusScheduled,
			Priority:        float32(i%1000) / 1000,
			URLRef:          ref,
			HTTPStatus:      u.status,
			FirstSeen:       100,
			DiscoverySource: meguri.SourceSeed,
		}); err != nil {
			tb.Fatalf("put: %v", err)
		}
		if _, ok := hostsSeen[hk]; !ok {
			hostsSeen[hk] = struct{}{}
			href, _ := s.Intern(u.host)
			s.PutHost(&meguri.HostRecord{HostKey: hk, HostRef: href, URLBudget: 64})
		}
	}
	return s
}

// TestCorpusCheckpointRecover is the M6 checkpoint-and-recover gate on the real
// slice: load every distinct URL, checkpoint to a .meguri snapshot, recover, and
// verify the count, a sample of records, and their interned URL strings all
// survive (doc 11 section 4, 5).
func TestCorpusCheckpointRecover(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	urls := loadCorpusURLs(t, path, 0)
	if len(urls) < 1000 {
		t.Fatalf("corpus too thin: %d distinct URLs", len(urls))
	}
	dir := t.TempDir()
	s := loadStore(t, dir, Options{Durability: DurabilityNormal}, urls)
	wantURLs := s.URLCount()
	wantHosts := s.HostCount()

	start := time.Now()
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	ckptMS := time.Since(start).Milliseconds()
	s.Close()

	snapBytes := snapshotSize(t, dir)
	start = time.Now()
	r, err := Open(dir, Options{Durability: DurabilityNormal})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer r.Close()
	recoverMS := time.Since(start).Milliseconds()

	if r.URLCount() != wantURLs || r.HostCount() != wantHosts {
		t.Fatalf("recovery lost rows: URLs %d->%d, hosts %d->%d", wantURLs, r.URLCount(), wantHosts, r.HostCount())
	}
	// Every record and its interned URL must survive the round trip.
	var checked int
	for i := range urls {
		if i%97 != 0 { // sample to keep the gate fast, every 97th URL
			continue
		}
		u := urls[i]
		rec, ok := r.GetURL(meguri.MakeURLKey(u.host, u.path))
		if !ok {
			t.Fatalf("recovery dropped %s", u.url)
		}
		if r.Str(rec.URLRef) != u.url {
			t.Fatalf("arena corrupted: got %q want %q", r.Str(rec.URLRef), u.url)
		}
		if rec.HTTPStatus != u.status {
			t.Fatalf("field corrupted for %s: status %d want %d", u.url, rec.HTTPStatus, u.status)
		}
		checked++
	}
	t.Logf("M6 checkpoint gate: %d URLs, %d hosts, snapshot %d bytes (%.1f B/URL), checkpoint %dms, recover %dms, %d records verified",
		wantURLs, wantHosts, snapBytes, float64(snapBytes)/float64(wantURLs), ckptMS, recoverMS, checked)
}

// TestCorpusLargerThanMemory is the M6 larger-than-memory gate on real records:
// load the slice under a resident budget far below its size, and check the
// resident set stays bounded while every real URL still reads back from the
// spilled log with its real fields (doc 11 section 6).
func TestCorpusLargerThanMemory(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	urls := loadCorpusURLs(t, path, 20000)
	if len(urls) < 5000 {
		t.Fatalf("corpus too thin: %d distinct URLs", len(urls))
	}
	const budget = 1024
	dir := t.TempDir()
	s := loadStore(t, dir, Options{Durability: DurabilityNone, ResidentBudget: budget}, urls)
	defer s.Close()

	if res := s.Resident(); res > budget {
		t.Fatalf("resident set %d exceeded budget %d", res, budget)
	}
	if s.URLCount() != len(urls) {
		t.Fatalf("eviction lost keys: count %d, loaded %d", s.URLCount(), len(urls))
	}
	// Read back a real sample; each one is almost certainly cold and pulled from
	// the log.
	var spilled int
	for i := 0; i < len(urls); i += 53 {
		u := urls[i]
		rec, ok := s.GetURL(meguri.MakeURLKey(u.host, u.path))
		if !ok || s.Str(rec.URLRef) != u.url {
			t.Fatalf("spilled read lost %s: ok=%v got %q", u.url, ok, s.Str(rec.URLRef))
		}
		spilled++
	}
	t.Logf("M6 larger-than-memory gate: %d URLs loaded, resident capped at %d (%.1f%% of the frontier), %d cold reads served from the log",
		len(urls), s.Resident(), 100*float64(budget)/float64(len(urls)), spilled)
}

// TestCorpusCheckpointReclaims is the M6 compaction gate on the real slice (doc 11
// section 4.2, audit 237): the log and string arena accumulate superseded frames
// and dead strings as records are rewritten, and the checkpoint rotation is what
// reclaims them. It loads the slice, then rewrites every record and re-interns its
// URL across many checkpoint generations, the churn that would grow an unrotated
// log without bound, and checks two reclamation properties after each generation.
//
// First, the directory holds exactly the live working set: the superblock, one
// snapshot, and one active log. An old generation's snapshot and log are removed on
// commit, so the file set never grows with the number of generations. Second, the
// on-disk footprint stays bounded: generation twelve sits within a small factor of
// generation one, proving the rotation reclaims the superseded state rather than
// letting it pile up. A store that kept every superseded frame would grow its
// footprint linearly in the generation count and fail both checks.
//
// This is the meguri side of compaction: the checkpoint subsumes a GC for M6. The
// continuous live-copy-forward compactor under epoch protection (Spec 2070 hashlog)
// is the external follow-up; the reclamation meguri itself owns is gated here.
func TestCorpusCheckpointReclaims(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	urls := loadCorpusURLs(t, path, 20000)
	if len(urls) < 5000 {
		t.Fatalf("corpus too thin: %d distinct URLs", len(urls))
	}
	dir := t.TempDir()
	s := loadStore(t, dir, Options{Durability: DurabilityNormal}, urls)
	defer s.Close()
	wantURLs := s.URLCount()

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("checkpoint gen 1: %v", err)
	}
	names, base := storeFootprint(t, dir)
	assertLiveFileSet(t, names, 1)

	// Churn many generations: each one rewrites every record with a changed field
	// and re-interns its URL, superseding the prior frames and orphaning the prior
	// arena bytes, then checkpoints. An unrotated log would grow with every pass.
	const generations = 12
	for gen := 2; gen <= generations; gen++ {
		for i := range urls {
			u := &urls[i]
			ref, err := s.Intern(u.url)
			if err != nil {
				t.Fatalf("re-intern gen %d: %v", gen, err)
			}
			if _, err := s.PutURL(&meguri.URLRecord{
				URLKey:          meguri.MakeURLKey(u.host, u.path),
				HostKey:         meguri.HostKeyOf(u.host),
				Status:          meguri.StatusScheduled,
				Priority:        float32((i+gen)%1000) / 1000,
				URLRef:          ref,
				HTTPStatus:      u.status,
				FirstSeen:       100,
				LastCrawled:     uint32(gen),
				DiscoverySource: meguri.SourceSeed,
			}); err != nil {
				t.Fatalf("re-put gen %d: %v", gen, err)
			}
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("checkpoint gen %d: %v", gen, err)
		}
		if s.URLCount() != wantURLs {
			t.Fatalf("gen %d changed the live count: %d, want %d", gen, s.URLCount(), wantURLs)
		}
		names, size := storeFootprint(t, dir)
		assertLiveFileSet(t, names, gen)
		// The footprint must not grow with generations. A 2x ceiling over generation
		// one absorbs the active log relative to the snapshot while still failing any
		// store that retained superseded generations.
		if size > 2*base {
			t.Fatalf("gen %d footprint %d bytes exceeded 2x the gen-1 footprint %d, rotation is not reclaiming",
				gen, size, base)
		}
	}

	// The reclaimed store still recovers the full live set byte for byte.
	s.Close()
	r, err := Open(dir, Options{Durability: DurabilityNormal})
	if err != nil {
		t.Fatalf("recover after churn: %v", err)
	}
	defer r.Close()
	if r.URLCount() != wantURLs {
		t.Fatalf("recovery after churn lost rows: %d, want %d", r.URLCount(), wantURLs)
	}
	var checked int
	for i := range urls {
		if i%97 != 0 {
			continue
		}
		u := urls[i]
		rec, ok := r.GetURL(meguri.MakeURLKey(u.host, u.path))
		if !ok || r.Str(rec.URLRef) != u.url {
			t.Fatalf("churned recovery lost %s: ok=%v got %q", u.url, ok, r.Str(rec.URLRef))
		}
		if rec.LastCrawled != uint32(generations) {
			t.Fatalf("recovered stale generation for %s: LastCrawled %d, want %d", u.url, rec.LastCrawled, generations)
		}
		checked++
	}
	_, finalSize := storeFootprint(t, dir)
	t.Logf("M6 reclamation gate: %d URLs over %d checkpoint generations, footprint held at %d bytes (gen-1 %d, %.2fx), %d records verified",
		wantURLs, generations, finalSize, base, float64(finalSize)/float64(base), checked)
}

// storeFootprint returns the names and total byte size of every file in a store
// directory, the on-disk cost a reclamation gate watches across generations.
func storeFootprint(tb testing.TB, dir string) (names []string, total int64) {
	tb.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		tb.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			tb.Fatal(err)
		}
		names = append(names, e.Name())
		total += info.Size()
	}
	return names, total
}

// assertLiveFileSet checks a store directory holds only the live working set after
// a checkpoint: the superblock, exactly one .meguri snapshot, and exactly one log.
// Any superseded snapshot or log still present is a reclamation leak.
func assertLiveFileSet(tb testing.TB, names []string, gen int) {
	tb.Helper()
	var snaps, logs, supers int
	for _, n := range names {
		switch {
		case strings.HasSuffix(n, ".meguri"):
			snaps++
		case strings.HasPrefix(n, "log-"):
			logs++
		case n == "super":
			supers++
		default:
			tb.Fatalf("gen %d: unexpected file %q in store dir", gen, n)
		}
	}
	if snaps != 1 || logs != 1 || supers != 1 {
		tb.Fatalf("gen %d: live file set not reclaimed, have %d snapshots %d logs %d superblocks (%v)",
			gen, snaps, logs, supers, names)
	}
}

// snapshotSize returns the byte size of the .meguri snapshot the checkpoint wrote.
func snapshotSize(tb testing.TB, dir string) int64 {
	tb.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		tb.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".meguri") {
			info, err := e.Info()
			if err != nil {
				tb.Fatal(err)
			}
			return info.Size()
		}
	}
	tb.Fatal("no .meguri snapshot written")
	return 0
}

// --- small URL helpers, no net/url, matching the prioritize corpus gate ---

func corpusHost(u string) string {
	_, rest, ok := strings.Cut(u, "://")
	if !ok {
		return ""
	}
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.IndexByte(rest, '@'); k >= 0 {
		rest = rest[k+1:]
	}
	if k := strings.IndexByte(rest, ':'); k >= 0 {
		rest = rest[:k]
	}
	return rest
}

func corpusPath(u string) string {
	_, rest, ok := strings.Cut(u, "://")
	if !ok {
		return u
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i:]
	}
	return "/"
}

func parseStatus(s string) uint16 {
	var v uint16
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0
		}
		v = v*10 + uint16(s[i]-'0')
	}
	return v
}
