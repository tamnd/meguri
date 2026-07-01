package live

import (
	"fmt"
	"path/filepath"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// TestCompactInsertUpdateCarry folds a delta of one insert and one update into a base
// file and checks the merged generation: the update replaces the base row's crawl
// state, the insert is a new key, every other base row is carried through with its URL
// string intact, and the counts add up. It runs across page sizes so the merge crosses
// URL-table page boundaries and the sequential arena reader resolves a multi-page walk.
func TestCompactInsertUpdateCarry(t *testing.T) {
	for _, pageRows := range []int{0, 16, 64} {
		t.Run(fmt.Sprintf("p%d", pageRows), func(t *testing.T) {
			const n, h = 200, 11
			items := makeItems(n, h)
			basePath := filepath.Join(t.TempDir(), "base.meguri")
			if _, err := BulkLoad(&sliceSource{items: items}, BuildOptions{
				Path:         basePath,
				TmpDir:       t.TempDir(),
				ExpectedKeys: n,
				RunRows:      17,
				PageRows:     pageRows,
				Codec:        format.CodecZstd,
				NowHours:     1000,
				Status:       m.StatusScheduled,
			}); err != nil {
				t.Fatalf("BulkLoad: %v", err)
			}

			delta := NewDelta()
			// Update an existing key: a recrawl that moved it to Crawled with a new due.
			upd := items[42]
			delta.Put(DeltaEntry{
				Rec: m.URLRecord{
					URLKey:     upd.Key,
					Status:     m.StatusCrawled,
					NextDue:    5000,
					FirstSeen:  1000,
					CrawlCount: 1,
					Priority:   0.5,
				},
				URL:  upd.URL,
				Host: upd.Host,
			})
			// Insert a brand new key on a host the base does not have.
			newHost := "fresh999.example.com"
			newPath := "/brand/new"
			newURL := "https://" + newHost + newPath
			newKey := m.URLKey{HostKey: m.HostKeyOf(newHost), PathKey: m.PathKeyOf(newPath)}
			delta.Put(DeltaEntry{
				Rec: m.URLRecord{
					URLKey:   newKey,
					Status:   m.StatusScheduled,
					Priority: 0.5,
				},
				URL:  newURL,
				Host: newHost,
			})

			outPath := filepath.Join(t.TempDir(), "gen2.meguri")
			res, err := Compact(basePath, delta, CompactOptions{
				OutPath:  outPath,
				TmpDir:   t.TempDir(),
				PageRows: pageRows,
				Codec:    format.CodecZstd,
				NowHours: 2000,
			})
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.URLCount != n+1 {
				t.Fatalf("URLCount = %d, want %d", res.URLCount, n+1)
			}
			if res.Inserted != 1 || res.Updated != 1 || res.Carried != n-1 {
				t.Fatalf("counts inserted=%d updated=%d carried=%d, want 1/1/%d", res.Inserted, res.Updated, res.Carried, n-1)
			}

			eng, err := Open(outPath)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer eng.Close()
			if eng.URLCount() != n+1 {
				t.Fatalf("engine URLCount = %d, want %d", eng.URLCount(), n+1)
			}

			arena, err := readArena(outPath)
			if err != nil {
				t.Fatalf("readArena: %v", err)
			}

			// Every original key is still present with its URL string, and the update
			// carries its new crawl state.
			for _, it := range items {
				rec, ok, err := eng.GetURL(it.Key)
				if err != nil || !ok {
					t.Fatalf("GetURL(%v) ok=%v err=%v", it.Key, ok, err)
				}
				if got := arenaSpan(arena, rec.URLRef); got != it.URL {
					t.Fatalf("url for %v = %q, want %q", it.Key, got, it.URL)
				}
			}
			updRec, ok, err := eng.GetURL(upd.Key)
			if err != nil || !ok {
				t.Fatalf("GetURL(update) ok=%v err=%v", ok, err)
			}
			if updRec.Status != m.StatusCrawled || updRec.NextDue != 5000 || updRec.CrawlCount != 1 {
				t.Fatalf("update not applied: status=%v nextDue=%d crawls=%d", updRec.Status, updRec.NextDue, updRec.CrawlCount)
			}

			// The insert is present with its string, and its FirstSeen was stamped from
			// NowHours since the delta left it zero.
			insRec, ok, err := eng.GetURL(newKey)
			if err != nil || !ok {
				t.Fatalf("GetURL(insert) ok=%v err=%v", ok, err)
			}
			if got := arenaSpan(arena, insRec.URLRef); got != newURL {
				t.Fatalf("insert url = %q, want %q", got, newURL)
			}
			if insRec.FirstSeen != 2000 {
				t.Fatalf("insert FirstSeen = %d, want 2000 (stamped from NowHours)", insRec.FirstSeen)
			}

			// A carried row keeps its base state (Scheduled), proving the merge copied
			// fields, not just keys.
			carried, _, _ := eng.GetURL(items[7].Key)
			if carried.Status != m.StatusScheduled {
				t.Fatalf("carried row status = %v, want Scheduled", carried.Status)
			}

			// An absent key still misses after compaction.
			absent := m.URLKey{HostKey: m.HostKeyOf("nope.invalid"), PathKey: m.PathKeyOf("/x")}
			if seen, err := eng.Seen(absent); err != nil || seen {
				t.Fatalf("absent seen=%v err=%v", seen, err)
			}
		})
	}
}
