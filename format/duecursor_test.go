package format

import (
	"testing"

	m "github.com/tamnd/meguri"
)

// TestDueCursor checks the bounded due scan agrees with the whole-column DueKeys
// reference at every now, across a small batch cap so the cursor crosses page
// boundaries and resumes mid-page, and that the file-level pushdown yields no
// batches when nothing is due. It runs the multi-page and single-page layouts so
// both the zone-pruned page walk and the whole-column fallback are covered.
func TestDueCursor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxPage int
	}{
		{"multipage", 2},
		{"singlepage", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := buildPartition(t, CodecZstd)
			p.MaxPageRows = tc.maxPage
			enc, err := Encode(p)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			r, err := NewReader(enc)
			if err != nil {
				t.Fatalf("reader: %v", err)
			}

			dmin, dmax := r.DueRange()
			// Below the soonest due time the pushdown must yield nothing.
			cur, err := r.DueCursor(dmin - 1)
			if err != nil {
				t.Fatalf("DueCursor pushdown: %v", err)
			}
			if b, _ := cur.NextBatch(4); b != nil {
				t.Fatalf("DueCursor(%d) returned %d keys, want none (pushed down)", dmin-1, len(b))
			}

			// At every now in and around the due range, the drained cursor equals the
			// reference DueKeys set. The cap is small so a batch ends mid-page.
			for now := dmin - 1; now <= dmax+1; now++ {
				want, err := r.DueKeys(now)
				if err != nil {
					t.Fatalf("DueKeys(%d): %v", now, err)
				}
				wantSet := map[m.URLKey]bool{}
				for _, k := range want {
					wantSet[k] = true
				}
				cur, err := r.DueCursor(now)
				if err != nil {
					t.Fatalf("DueCursor(%d): %v", now, err)
				}
				gotSet := map[m.URLKey]bool{}
				total := 0
				for {
					batch, err := cur.NextBatch(2)
					if err != nil {
						t.Fatalf("NextBatch(%d): %v", now, err)
					}
					if batch == nil {
						break
					}
					if len(batch) > 2 {
						t.Fatalf("batch of %d exceeds cap 2", len(batch))
					}
					for _, k := range batch {
						if gotSet[k] {
							t.Fatalf("DueCursor(%d) returned duplicate key %v", now, k)
						}
						gotSet[k] = true
						total++
					}
				}
				if total != len(want) {
					t.Fatalf("DueCursor(%d) returned %d keys, DueKeys says %d", now, total, len(want))
				}
				for k := range gotSet {
					if !wantSet[k] {
						t.Fatalf("DueCursor(%d) returned key %v not in DueKeys", now, k)
					}
				}
			}
		})
	}
}
