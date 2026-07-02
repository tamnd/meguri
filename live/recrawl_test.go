package live

import (
	"fmt"
	"path/filepath"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/freshness"
)

// TestRecrawlFold folds a crawl outcome into every due row of a base file and checks the
// new generation: every due row moved to a rescheduled state with its crawl count bumped
// and a next due time in the future, every row's URL string survives, the counts add up,
// and the file is a valid drop-in base. It runs across page sizes so the streaming fold
// crosses URL-table page boundaries and the sequential arena reader resolves a multi-page
// walk.
func TestRecrawlFold(t *testing.T) {
	for _, pageRows := range []int{0, 16, 64} {
		t.Run(fmt.Sprintf("p%d", pageRows), func(t *testing.T) {
			const n, h = 200, 11
			const nowBuild, nowRecrawl = 1000, 2000
			items := makeItems(n, h)
			basePath := filepath.Join(t.TempDir(), "base.meguri")
			if _, err := BulkLoad(&sliceSource{items: items}, BuildOptions{
				Path:         basePath,
				TmpDir:       t.TempDir(),
				ExpectedKeys: n,
				RunRows:      17,
				PageRows:     pageRows,
				Codec:        format.CodecZstd,
				NowHours:     nowBuild,
				Status:       m.StatusScheduled,
			}); err != nil {
				t.Fatalf("BulkLoad: %v", err)
			}

			outPath := filepath.Join(t.TempDir(), "gen2.meguri")
			res, err := Recrawl(basePath, RecrawlOptions{
				OutPath:    outPath,
				TmpDir:     t.TempDir(),
				PageRows:   pageRows,
				Codec:      format.CodecZstd,
				NowHours:   nowRecrawl,
				Params:     freshness.DefaultParams(),
				Tau:        1e-4,
				ChangeRate: 0.25,
				Seed:       7,
			})
			if err != nil {
				t.Fatalf("Recrawl: %v", err)
			}

			// Every row was due (BulkLoad set NextDue = nowBuild <= nowRecrawl), so all are
			// recrawled and none carried, and the total is preserved.
			if res.URLCount != n {
				t.Fatalf("URLCount = %d, want %d", res.URLCount, n)
			}
			if res.Recrawled != n || res.Carried != 0 {
				t.Fatalf("recrawled=%d carried=%d, want %d/0", res.Recrawled, res.Carried, n)
			}
			if res.Changed+res.NoChange != n {
				t.Fatalf("changed=%d nochange=%d sum != %d", res.Changed, res.NoChange, n)
			}
			if res.Changed == 0 || res.NoChange == 0 {
				t.Fatalf("expected a mix of change and no-change outcomes, got changed=%d nochange=%d", res.Changed, res.NoChange)
			}
			if res.MeanLambda <= 0 {
				t.Fatalf("MeanLambda = %v, want positive", res.MeanLambda)
			}

			eng, err := Open(outPath)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer eng.Close()
			if eng.URLCount() != n {
				t.Fatalf("engine URLCount = %d, want %d", eng.URLCount(), n)
			}

			arena, err := readArena(outPath)
			if err != nil {
				t.Fatalf("readArena: %v", err)
			}

			// Every row survived with its URL string, moved off Scheduled onto a recrawl
			// state, bumped its crawl count to 2, and was rescheduled strictly ahead of now.
			for _, it := range items {
				rec, ok, err := eng.GetURL(it.Key)
				if err != nil || !ok {
					t.Fatalf("GetURL(%v) ok=%v err=%v", it.Key, ok, err)
				}
				if got := arenaSpan(arena, rec.URLRef); got != it.URL {
					t.Fatalf("url for %v = %q, want %q", it.Key, got, it.URL)
				}
				if rec.CrawlCount != 1 {
					t.Fatalf("CrawlCount for %v = %d, want 1 (base was 0, one fold)", it.Key, rec.CrawlCount)
				}
				if rec.LastCrawled != nowRecrawl {
					t.Fatalf("LastCrawled for %v = %d, want %d", it.Key, rec.LastCrawled, nowRecrawl)
				}
				if rec.Status != m.StatusDueRecrawl {
					t.Fatalf("status for %v = %v, want DueRecrawl", it.Key, rec.Status)
				}
				if rec.NextDue <= nowRecrawl {
					t.Fatalf("NextDue for %v = %d, want > %d", it.Key, rec.NextDue, nowRecrawl)
				}
			}

			// A second recrawl with a now before the fresh due times finds nothing due, so
			// every row carries through untouched: the schedule really moved forward.
			out2 := filepath.Join(t.TempDir(), "gen3.meguri")
			res2, err := Recrawl(outPath, RecrawlOptions{
				OutPath:    out2,
				TmpDir:     t.TempDir(),
				PageRows:   pageRows,
				Codec:      format.CodecZstd,
				NowHours:   nowRecrawl, // before any of the new NextDue times
				Params:     freshness.DefaultParams(),
				Tau:        1e-4,
				ChangeRate: 0.25,
				Seed:       9,
			})
			if err != nil {
				t.Fatalf("Recrawl 2: %v", err)
			}
			if res2.Recrawled != 0 || res2.Carried != n {
				t.Fatalf("second recrawl recrawled=%d carried=%d, want 0/%d", res2.Recrawled, res2.Carried, n)
			}
		})
	}
}
