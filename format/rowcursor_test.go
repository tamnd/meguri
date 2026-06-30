package format

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	m "github.com/tamnd/meguri"
)

// TestURLRowCursor is the sequential-read gate for the checkpoint body-store
// merge (doc 14): a cursor over the URL table must yield every stored record once,
// in stored order, byte-equal to the encoded input, across single-page and
// multi-page tables and both codecs. This is the source the checkpoint reads base
// bodies from in place of the random-read log gather.
func TestURLRowCursor(t *testing.T) {
	for _, codec := range []uint8{CodecNone, CodecZstd} {
		for _, n := range []int{0, 1, 6, 100, 257} {
			for _, maxRows := range []int{0, 1, 5, 16, 1000} {
				want := makeURLRecords(n)
				p := streamTestPartition(codec, maxRows, want)
				path := filepath.Join(t.TempDir(), "rows.meguri")
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
				cur, err := r.URLRows()
				if err != nil {
					t.Fatalf("codec=%d n=%d maxRows=%d: cursor: %v", codec, n, maxRows, err)
				}
				var got []m.URLRecord
				for {
					rec, ok, err := cur.Next()
					if err != nil {
						t.Fatalf("codec=%d n=%d maxRows=%d: next: %v", codec, n, maxRows, err)
					}
					if !ok {
						break
					}
					got = append(got, rec)
				}
				if len(got) != len(want) {
					t.Fatalf("codec=%d n=%d maxRows=%d: yielded %d rows, want %d", codec, n, maxRows, len(got), len(want))
				}
				for i := range want {
					if !reflect.DeepEqual(got[i], want[i]) {
						t.Fatalf("codec=%d n=%d maxRows=%d: row %d mismatch\n got  %+v\n want %+v", codec, n, maxRows, i, got[i], want[i])
					}
				}
			}
		}
	}
}
