package format

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	m "github.com/tamnd/meguri"
)

// urlHostRange returns the min and max HostKey across the records, the header
// HostKey bounds a real partition carries. An empty set spans the whole range.
func urlHostRange(recs []m.URLRecord) (lo, hi uint64) {
	if len(recs) == 0 {
		return 0, ^uint64(0)
	}
	lo, hi = recs[0].URLKey.HostKey, recs[0].URLKey.HostKey
	for i := range recs {
		h := recs[i].URLKey.HostKey
		if h < lo {
			lo = h
		}
		if h > hi {
			hi = h
		}
	}
	return lo, hi
}

// TestLookupURL is the point-read gate for the file-as-body-store path (doc 03
// change 3): once the .meguri file holds the base records, the live store
// resolves a GetURL miss by key against the file, so LookupURL must return the
// exact stored record for every present key and a clean miss for an absent one.
// It runs across several row counts and page caps, so both the single-page decode
// path (maxRows 0) and the multi-page candidate scan (small caps that split a host
// across pages) are exercised, and across both codecs.
func TestLookupURL(t *testing.T) {
	for _, codec := range []uint8{CodecNone, CodecZstd} {
		for _, n := range []int{0, 1, 6, 100, 257} {
			for _, maxRows := range []int{0, 1, 5, 16, 1000} {
				recs := makeURLRecords(n)
				p := streamTestPartition(codec, maxRows, recs)
				// A real partition's header HostKey range covers every URL it holds, so
				// widen the synthetic range (it is otherwise the two host records' span)
				// to bound the URL hosts, the precondition LookupURL's pushdown relies on.
				p.HostKeyLo, p.HostKeyHi = urlHostRange(recs)
				path := filepath.Join(t.TempDir(), "lookup.meguri")
				if err := EncodeToFile(path, p); err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: encode: %v", codec, n, maxRows, err)
				}
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				r, err := NewReader(b)
				if err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: reader: %v", codec, n, maxRows, err)
				}

				// Every stored key resolves to its exact record.
				for i := range recs {
					got, ok, err := r.LookupURL(recs[i].URLKey)
					if err != nil {
						t.Fatalf("codec=%d n=%d maxRows=%d: lookup %v: %v", codec, n, maxRows, recs[i].URLKey, err)
					}
					if !ok {
						t.Fatalf("codec=%d n=%d maxRows=%d: key %v missing", codec, n, maxRows, recs[i].URLKey)
					}
					if !reflect.DeepEqual(got, recs[i]) {
						t.Fatalf("codec=%d n=%d maxRows=%d: key %v record mismatch\n got  %+v\n want %+v", codec, n, maxRows, recs[i].URLKey, got, recs[i])
					}
				}

				// A key the partition does not hold misses cleanly. The host is in
				// range (one of the seeded hosts) but the path is never stored.
				absent := m.MakeURLKey("example.com", "/never-stored-path")
				if _, ok, err := r.LookupURL(absent); err != nil || ok {
					t.Fatalf("codec=%d n=%d maxRows=%d: absent key returned ok=%v err=%v", codec, n, maxRows, ok, err)
				}

				// A key whose host is outside the partition range is rejected by the
				// header pushdown with no body decode.
				outOfRange := m.URLKey{HostKey: r.header.HostKeyHi + 1, PathKey: 0}
				if r.header.HostKeyHi != ^uint64(0) {
					if _, ok, err := r.LookupURL(outOfRange); err != nil || ok {
						t.Fatalf("codec=%d n=%d maxRows=%d: out-of-range key returned ok=%v err=%v", codec, n, maxRows, ok, err)
					}
				}
			}
		}
	}
}
