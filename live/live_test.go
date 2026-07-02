package live

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// sliceSource is a Source over an in-memory item slice for the tests.
type sliceSource struct {
	items []Item
	i     int
}

func (s *sliceSource) Next() (Item, bool, error) {
	if s.i >= len(s.items) {
		return Item{}, false, nil
	}
	it := s.items[s.i]
	s.i++
	return it, true, nil
}

// makeItems builds n distinct URLs spread across h hosts, keyed the way the engine
// keys them, so the test exercises the real host-clustered sort order.
func makeItems(n, h int) []Item {
	items := make([]Item, 0, n)
	for i := range n {
		host := fmt.Sprintf("host%03d.example.com", i%h)
		path := fmt.Sprintf("/page/%d", i)
		url := "https://" + host + path
		items = append(items, Item{
			Key:  m.URLKey{HostKey: m.HostKeyOf(host), PathKey: m.PathKeyOf(path)},
			URL:  url,
			Host: host,
		})
	}
	return items
}

func TestBulkLoadOpenDedup(t *testing.T) {
	for _, tc := range []struct {
		n, h, pageRows int
	}{
		{1, 1, 0},
		{6, 2, 0},
		{100, 7, 0},
		{100, 7, 16},
		{1000, 23, 64},
	} {
		t.Run(fmt.Sprintf("n%d_h%d_p%d", tc.n, tc.h, tc.pageRows), func(t *testing.T) {
			items := makeItems(tc.n, tc.h)
			path := filepath.Join(t.TempDir(), "p.meguri")
			res, err := BulkLoad(&sliceSource{items: items}, BuildOptions{
				Path:         path,
				TmpDir:       t.TempDir(),
				ExpectedKeys: uint64(tc.n),
				RunRows:      17, // tiny so the external sort spills several runs
				PageRows:     tc.pageRows,
				Codec:        format.CodecZstd,
				NowHours:     1000,
			})
			if err != nil {
				t.Fatalf("BulkLoad: %v", err)
			}
			if res.URLCount != tc.n {
				t.Fatalf("URLCount = %d, want %d", res.URLCount, tc.n)
			}
			eng, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer eng.Close()
			if eng.URLCount() != tc.n {
				t.Fatalf("engine URLCount = %d, want %d", eng.URLCount(), tc.n)
			}
			// Every present key resolves through the filter-then-base path, with its
			// stored string recoverable from the file's arena.
			arena, err := readArena(path)
			if err != nil {
				t.Fatalf("readArena: %v", err)
			}
			for _, it := range items {
				seen, err := eng.Seen(it.Key)
				if err != nil {
					t.Fatalf("Seen(%v): %v", it.Key, err)
				}
				if !seen {
					t.Fatalf("present key %v not seen", it.Key)
				}
				rec, ok, err := eng.GetURL(it.Key)
				if err != nil || !ok {
					t.Fatalf("GetURL(%v) ok=%v err=%v", it.Key, ok, err)
				}
				if got := arenaSpan(arena, rec.URLRef); got != it.URL {
					t.Fatalf("url for %v = %q, want %q", it.Key, got, it.URL)
				}
			}
			// An absent key misses. The filter may false-positive, but the base
			// confirms, so Seen must be false for a key never inserted.
			absent := m.URLKey{HostKey: m.HostKeyOf("nope.invalid"), PathKey: m.PathKeyOf("/x")}
			if seen, err := eng.Seen(absent); err != nil || seen {
				t.Fatalf("absent key seen=%v err=%v", seen, err)
			}
		})
	}
}

// TestBulkLoadSeenFilterIsRibbon checks the seal writes the ribbon form into the
// seen-set region (kind byte 1, not the blocked Bloom's 0), that the width tracks
// the FPRate through RibbonBitsForFPR, and that the engine loads it and honors the
// one-sided contract: every built key probes seen with no false negative.
func TestBulkLoadSeenFilterIsRibbon(t *testing.T) {
	items := makeItems(5000, 41)
	path := filepath.Join(t.TempDir(), "p.meguri")
	if _, err := BulkLoad(&sliceSource{items: items}, BuildOptions{
		Path:         path,
		TmpDir:       t.TempDir(),
		ExpectedKeys: uint64(len(items)),
		Codec:        format.CodecZstd,
		FPRate:       1e-4, // r=14
		NowHours:     1000,
	}); err != nil {
		t.Fatalf("BulkLoad: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	r, err := format.NewReader(raw)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	fb, err := r.SeenFilter()
	if err != nil {
		t.Fatalf("SeenFilter: %v", err)
	}
	if len(fb) < 2 || fb[1] != 1 { // 1 == dedup.filterKindRibbon
		t.Fatalf("seen filter kind = %d, want ribbon (1)", fb[1])
	}

	eng, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()
	for _, it := range items {
		seen, err := eng.Seen(it.Key)
		if err != nil {
			t.Fatalf("Seen(%v): %v", it.Key, err)
		}
		if !seen {
			t.Fatalf("built key %v probed new, the one-sided contract broke", it.Key)
		}
	}
	// r=14 at the 0.90 band load lands near 15.6 bits/url; assert it sits in the
	// r-bit band, well under the blocked-Bloom's 22.
	if bpu := eng.BitsPerURL(); bpu <= 0 || bpu > 18 {
		t.Fatalf("ribbon bits/url = %.2f, outside the r=14 band", bpu)
	}
}

// TestEngineTrustFilterSkipsBase is the M3 contract: opened WithTrustFilter, a
// filter hit returns seen without ever touching the base, so every present key still
// probes seen (the one-sided guarantee holds) but BaseProbes stays zero. The default
// engine confirms each hit against the base, so it does record base probes. An
// absent key the filter rules out costs no base access in either mode.
func TestEngineTrustFilterSkipsBase(t *testing.T) {
	items := makeItems(4000, 31)
	path := filepath.Join(t.TempDir(), "p.meguri")
	if _, err := BulkLoad(&sliceSource{items: items}, BuildOptions{
		Path:         path,
		TmpDir:       t.TempDir(),
		ExpectedKeys: uint64(len(items)),
		Codec:        format.CodecZstd,
		FPRate:       1e-4,
		NowHours:     1000,
	}); err != nil {
		t.Fatalf("BulkLoad: %v", err)
	}

	// Trust mode: every member seen, no base probes at all.
	trust, err := Open(path, WithTrustFilter())
	if err != nil {
		t.Fatalf("Open trust: %v", err)
	}
	defer trust.Close()
	for _, it := range items {
		seen, err := trust.Seen(it.Key)
		if err != nil {
			t.Fatalf("trust Seen(%v): %v", it.Key, err)
		}
		if !seen {
			t.Fatalf("trust mode dropped present key %v, one-sided contract broke", it.Key)
		}
	}
	if bp := trust.BaseProbes(); bp != 0 {
		t.Fatalf("trust mode made %d base probes, want 0 (it must never touch the base on a hit)", bp)
	}

	// Default mode: same present keys, but each hit is confirmed against the base,
	// so the base-probe count is the hit count.
	confirm, err := Open(path)
	if err != nil {
		t.Fatalf("Open confirm: %v", err)
	}
	defer confirm.Close()
	for _, it := range items {
		if _, err := confirm.Seen(it.Key); err != nil {
			t.Fatalf("confirm Seen(%v): %v", it.Key, err)
		}
	}
	if bp := confirm.BaseProbes(); bp != uint64(len(items)) {
		t.Fatalf("confirm mode made %d base probes, want %d (one per present hit)", bp, len(items))
	}

	// An absent key the filter rules out short-circuits before the base in both
	// modes, so trust made no base probes for it either.
	absent := m.URLKey{HostKey: m.HostKeyOf("nope.invalid"), PathKey: m.PathKeyOf("/x")}
	if seen, err := trust.Seen(absent); err != nil || seen {
		t.Fatalf("trust absent key seen=%v err=%v", seen, err)
	}
	if bp := trust.BaseProbes(); bp != 0 {
		t.Fatalf("trust mode touched the base for a filter-miss key, base probes = %d", bp)
	}
}

// readArena decodes the file's string region the same way a reader would, so the
// test can resolve a record's URLRef back to its string.
func readArena(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	part, err := format.Decode(raw)
	if err != nil {
		return nil, err
	}
	return part.Strings, nil
}

// arenaSpan reads the uvarint-prefixed span at off, matching format/blob.go.
func arenaSpan(arena []byte, off uint64) string {
	if off == 0 || off >= uint64(len(arena)) {
		return ""
	}
	n, k := binary.Uvarint(arena[off:])
	if k <= 0 {
		return ""
	}
	start := off + uint64(k)
	end := start + n
	if end > uint64(len(arena)) {
		return ""
	}
	return string(arena[start:end])
}
