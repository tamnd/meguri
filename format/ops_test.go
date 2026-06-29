package format

import (
	"reflect"
	"testing"

	m "github.com/tamnd/meguri"
)

// buildOpsPartition makes a small partition with explicit HostKeys so a test can
// split it at a known boundary. Unlike buildPartition in file_test.go, it interns
// every string into a sentinel-led arena (newArena/arenaIntern), so a HostRef of
// 0 means "none" and the rebaser in ops.go handles the references correctly. The
// hosts carry HostKeys 10, 20, 30 with two URL rows each, all rows sorted by
// URLKey and each host's rows contiguous.
func buildOpsPartition(t *testing.T) *Partition {
	t.Helper()
	arena := newArena()
	intern := func(s string) uint64 {
		var off uint64
		arena, off = arenaIntern(arena, []byte(s))
		return off
	}

	hostKeys := []uint64{10, 20, 30}
	hosts := make([]m.HostRecord, 0, len(hostKeys))
	var urls []m.URLRecord
	for hi, hk := range hostKeys {
		host := []string{"a.example", "b.example", "c.example"}[hi]
		ref := intern(host)
		hosts = append(hosts, m.HostRecord{
			HostKey:        hk,
			HostRef:        ref,
			Grouping:       m.GroupFullHost,
			RegistrableRef: ref,
			CrawlDelay:     10,
			CrawlTotal:     uint32(hi + 1),
			URLCount:       2,
		})
		for p := range 2 {
			path := []string{"/x", "/y"}[p]
			urls = append(urls, m.URLRecord{
				URLKey:      m.URLKey{HostKey: hk, PathKey: uint64(p + 1)},
				HostKey:     hk,
				Status:      m.StatusCrawled,
				URLRef:      intern("http://" + host + path),
				FirstSeen:   480000,
				LastCrawled: 480000,
				NextDue:     480024,
				CrawlCount:  1,
				HTTPStatus:  200,
			})
		}
	}
	return &Partition{
		ID:           7,
		HostKeyLo:    hostKeys[0],
		HostKeyHi:    hostKeys[len(hostKeys)-1],
		CreatedHours: 480000,
		DefaultCodec: CodecZstd,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      arena,
	}
}

// encDecEqual checks a partition survives an encode/decode round trip, the way a
// caller persists an ops result and reads it back.
func encDecEqual(t *testing.T, p *Partition) {
	t.Helper()
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.URLs) != len(p.URLs) || len(got.Hosts) != len(p.Hosts) {
		t.Fatalf("counts changed: urls %d->%d hosts %d->%d",
			len(p.URLs), len(got.URLs), len(p.Hosts), len(got.Hosts))
	}
	for i := range p.URLs {
		if resolveURL(p, p.URLs[i].URLRef) != resolveURL(got, got.URLs[i].URLRef) {
			t.Fatalf("url %d string changed: %q vs %q",
				i, resolveURL(p, p.URLs[i].URLRef), resolveURL(got, got.URLs[i].URLRef))
		}
	}
}

func resolveURL(p *Partition, off uint64) string {
	return string(arenaRead(p.Strings, off))
}

func TestSplitKeepsHostsWhole(t *testing.T) {
	p := buildOpsPartition(t)
	lo, hi := Split(p, 20)

	if len(lo.Hosts) != 1 || lo.Hosts[0].HostKey != 10 {
		t.Fatalf("lo half hosts wrong: %+v", lo.Hosts)
	}
	if len(hi.Hosts) != 2 || hi.Hosts[0].HostKey != 20 {
		t.Fatalf("hi half hosts wrong: %+v", hi.Hosts)
	}
	if len(lo.URLs) != 2 || len(hi.URLs) != 4 {
		t.Fatalf("url split wrong: lo %d hi %d", len(lo.URLs), len(hi.URLs))
	}
	if lo.HostKeyHi != 19 || hi.HostKeyLo != 20 {
		t.Fatalf("split ranges wrong: lo hi %d, hi lo %d", lo.HostKeyHi, hi.HostKeyLo)
	}
	// Each half's arena must still resolve its own rows and round trip.
	if resolveURL(lo, lo.URLs[0].URLRef) != "http://a.example/x" {
		t.Fatalf("lo string lost: %q", resolveURL(lo, lo.URLs[0].URLRef))
	}
	if resolveURL(hi, hi.URLs[0].URLRef) != "http://b.example/x" {
		t.Fatalf("hi string lost: %q", resolveURL(hi, hi.URLs[0].URLRef))
	}
	encDecEqual(t, lo)
	encDecEqual(t, hi)
}

func TestSplitMergeRoundTrip(t *testing.T) {
	p := buildOpsPartition(t)
	lo, hi := Split(p, 20)
	merged, err := Merge(lo, hi)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(merged.URLs) != len(p.URLs) || len(merged.Hosts) != len(p.Hosts) {
		t.Fatalf("merge changed counts: urls %d hosts %d", len(merged.URLs), len(merged.Hosts))
	}
	if merged.HostKeyLo != p.HostKeyLo || merged.HostKeyHi != p.HostKeyHi {
		t.Fatalf("merge range wrong: [%d,%d]", merged.HostKeyLo, merged.HostKeyHi)
	}
	for i := range p.URLs {
		want := resolveURL(p, p.URLs[i].URLRef)
		got := resolveURL(merged, merged.URLs[i].URLRef)
		if want != got {
			t.Fatalf("merge url %d string changed: %q vs %q", i, want, got)
		}
	}
	encDecEqual(t, merged)
}

func TestMergeRejectsOverlap(t *testing.T) {
	a := buildOpsPartition(t)
	b := buildOpsPartition(t) // same HostKeys, so the merge overlaps
	if _, err := Merge(a, b); err != ErrNotSorted {
		t.Fatalf("merge of overlapping partitions: want ErrNotSorted, got %v", err)
	}
}

func TestCompactDropsGone(t *testing.T) {
	p := buildOpsPartition(t)
	// Mark the second host's first row Gone; compact should drop it and its
	// string should fall out of the arena, but the surviving rows keep theirs.
	for i := range p.URLs {
		if p.URLs[i].URLKey.HostKey == 20 && p.URLs[i].URLKey.PathKey == 1 {
			p.URLs[i].Status = m.StatusGone
		}
	}
	out := Compact(p)
	if len(out.URLs) != len(p.URLs)-1 {
		t.Fatalf("compact kept gone row: %d -> %d", len(p.URLs), len(out.URLs))
	}
	for _, r := range out.URLs {
		if r.Status == m.StatusGone {
			t.Fatalf("gone row survived compact")
		}
		if resolveURL(out, r.URLRef) == "" {
			t.Fatalf("live row lost its string after compact")
		}
	}
	// The dropped row's arena should have shrunk the partition (one URL string and
	// nothing else gone), so compact's arena is strictly smaller than the input's.
	if len(out.Strings) >= len(p.Strings) {
		t.Fatalf("compact did not reclaim arena: %d -> %d", len(p.Strings), len(out.Strings))
	}
	encDecEqual(t, out)
}

func TestCompactNoGoneIsIdentity(t *testing.T) {
	p := buildOpsPartition(t)
	out := Compact(p)
	if len(out.URLs) != len(p.URLs) {
		t.Fatalf("compact dropped live rows: %d -> %d", len(p.URLs), len(out.URLs))
	}
	// Compact rebases the arena, so the *Ref offsets legitimately shift; the
	// invariant is that every resolved string and every non-string field is
	// unchanged when no rows are dropped.
	for i := range p.URLs {
		a, b := p.URLs[i], out.URLs[i]
		if resolveURL(p, a.URLRef) != resolveURL(out, b.URLRef) {
			t.Fatalf("compact altered url %d string", i)
		}
		a.URLRef, a.ETagRef, a.RedirectRef = 0, 0, 0
		b.URLRef, b.ETagRef, b.RedirectRef = 0, 0, 0
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("compact altered url %d non-string fields", i)
		}
	}
}

// TestOpsOnCorpus is the ops gate on real data: split the frozen ccrawl slice at
// its median host, require each half encodes and decodes, then merge the halves
// back and require the merged partition matches the original counts and round
// trips. This runs the rebalance primitives on real host and URL distributions,
// not a synthetic handful.
func TestOpsOnCorpus(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(t, path)
	if len(p.Hosts) < 2 {
		t.Skipf("corpus has %d hosts, need at least 2 to split", len(p.Hosts))
	}
	mid := p.Hosts[len(p.Hosts)/2].HostKey
	lo, hi := Split(p, mid)
	if len(lo.URLs)+len(hi.URLs) != len(p.URLs) {
		t.Fatalf("split lost urls: %d + %d != %d", len(lo.URLs), len(hi.URLs), len(p.URLs))
	}
	if len(lo.Hosts)+len(hi.Hosts) != len(p.Hosts) {
		t.Fatalf("split lost hosts: %d + %d != %d", len(lo.Hosts), len(hi.Hosts), len(p.Hosts))
	}
	encDecEqual(t, lo)
	encDecEqual(t, hi)

	merged, err := Merge(lo, hi)
	if err != nil {
		t.Fatalf("merge corpus halves: %v", err)
	}
	if len(merged.URLs) != len(p.URLs) || len(merged.Hosts) != len(p.Hosts) {
		t.Fatalf("merge changed counts: urls %d hosts %d", len(merged.URLs), len(merged.Hosts))
	}
	encDecEqual(t, merged)
	t.Logf("ops on corpus: split at host %d into %d/%d urls, merged back to %d",
		mid, len(lo.URLs), len(hi.URLs), len(merged.URLs))
}
