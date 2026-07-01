package seed

import (
	"fmt"
	"path/filepath"
	"testing"
)

// makeURLs returns n deterministic, lexically sorted URLs with shared prefixes,
// mimicking the sorted, host-clustered corpus.
func makeURLs(n int) []string {
	out := make([]string, 0, n)
	for h := 0; len(out) < n; h++ {
		host := fmt.Sprintf("https://host%06d.example.com", h)
		for p := 0; p < 37 && len(out) < n; p++ {
			out = append(out, fmt.Sprintf("%s/path/segment/%05d/index.html", host, p))
		}
	}
	return out
}

func writeSeed(t *testing.T, path string, urls []string, opts WriterOptions) {
	t.Helper()
	w, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, u := range urls {
		if err := w.AddString(u); err != nil {
			t.Fatalf("Add %q: %v", u, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func readAll(t *testing.T, path string) []string {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	var got []string
	for i := 0; i < r.Blocks(); i++ {
		br, err := r.BlockReader(i)
		if err != nil {
			t.Fatalf("BlockReader(%d): %v", i, err)
		}
		for {
			u, ok := br.Next()
			if !ok {
				break
			}
			got = append(got, string(u))
		}
	}
	return got
}

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		n     int
		bs    int
		codec Codec
	}{
		{"raw_small_block", 1000, 4096, CodecRaw},
		{"raw_default_block", 5000, DefaultBlockSize, CodecRaw},
		{"raw_tiny_block", 200, 256, CodecRaw},
		{"zstd_small_block", 1000, 4096, CodecZstd},
		{"zstd_default_block", 5000, DefaultBlockSize, CodecZstd},
		{"empty", 0, 4096, CodecRaw},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			urls := makeURLs(tc.n)
			path := filepath.Join(t.TempDir(), "s.mgs")
			writeSeed(t, path, urls, WriterOptions{BlockSize: tc.bs, Codec: tc.codec, HostLo: 10, HostHi: 99})
			got := readAll(t, path)
			if len(got) != len(urls) {
				t.Fatalf("record count: got %d want %d", len(got), len(urls))
			}
			for i := range urls {
				if got[i] != urls[i] {
					t.Fatalf("record %d: got %q want %q", i, got[i], urls[i])
				}
			}
			r, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer r.Close()
			if r.RecordCount() != uint64(len(urls)) {
				t.Fatalf("RecordCount: got %d want %d", r.RecordCount(), len(urls))
			}
			lo, hi := r.HostRange()
			if lo != 10 || hi != 99 {
				t.Fatalf("HostRange: got %d..%d want 10..99", lo, hi)
			}
		})
	}
}

// TestBlockSplit proves a worker handed a disjoint block range reads exactly that
// range's records and nothing else, the property the parallel driver relies on.
func TestBlockSplit(t *testing.T) {
	urls := makeURLs(5000)
	path := filepath.Join(t.TempDir(), "s.mgs")
	writeSeed(t, path, urls, WriterOptions{BlockSize: 2048, Codec: CodecRaw})
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	if r.Blocks() < 2 {
		t.Fatalf("want a multi-block file, got %d blocks", r.Blocks())
	}
	// Read the whole file in two halves of the block range and check the
	// concatenation reproduces the corpus in order.
	mid := r.Blocks() / 2
	var got []string
	for _, rng := range [][2]int{{0, mid}, {mid, r.Blocks()}} {
		for i := rng[0]; i < rng[1]; i++ {
			br, err := r.BlockReader(i)
			if err != nil {
				t.Fatalf("BlockReader(%d): %v", i, err)
			}
			for {
				u, ok := br.Next()
				if !ok {
					break
				}
				got = append(got, string(u))
			}
		}
	}
	if len(got) != len(urls) {
		t.Fatalf("split read count: got %d want %d", len(got), len(urls))
	}
	for i := range urls {
		if got[i] != urls[i] {
			t.Fatalf("split record %d: got %q want %q", i, got[i], urls[i])
		}
	}
	// FirstKey of each block must be the lexically smallest URL in it (sorted input),
	// so a binary search over first keys locates a range.
	for i := 0; i < r.Blocks(); i++ {
		if i > 0 && string(r.FirstKey(i)) <= string(r.FirstKey(i-1)) {
			t.Fatalf("block %d first key not ascending", i)
		}
	}
}
