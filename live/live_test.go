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
