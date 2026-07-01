package dataset

import (
	"path/filepath"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/live"
)

// TestIncrementalDump exports a base, then exports a second .meguri whose one
// overlapping URL has a newer state into the same repo. The second dump must append
// files (not clobber the first), advance the watermark, and on import the overlapping
// key must resolve to the newer state while the untouched keys survive.
func TestIncrementalDump(t *testing.T) {
	dir := t.TempDir()
	repo := filepath.Join(dir, "repo")

	base := sampleItems(t)
	fileA := filepath.Join(dir, "a.meguri")
	buildMeguri(t, fileA, base)
	st1, err := ExportRepo(fileA, repo, ExportOptions{FileRows: 100})
	if err != nil {
		t.Fatalf("base export: %v", err)
	}
	if st1.Files != 1 {
		t.Fatalf("base should be one file, got %d", st1.Files)
	}

	// A second store: the same key set, but b.example/ was recrawled and changed at a
	// later hour, so its activity is past the base watermark.
	changed := sampleItems(t)
	var target m.URLKey
	for i := range changed {
		if changed[i].Host == "b.example" && changed[i].URL == "https://b.example/" {
			changed[i].Rec.LastChanged = 9000
			changed[i].Rec.LastCrawled = 9000
			changed[i].Rec.ChangeCount = 99
			changed[i].Rec.CrawlCount = 100
			target = changed[i].Rec.URLKey
		}
	}
	fileC := filepath.Join(dir, "c.meguri")
	buildMeguri(t, fileC, changed)

	// Incremental: only rows with activity at or after the base watermark+1.
	man0, err := ReadManifest(repo)
	if err != nil {
		t.Fatalf("read base manifest: %v", err)
	}
	st2, err := ExportRepo(fileC, repo, ExportOptions{FileRows: 100, SinceHours: man0.Watermark + 1})
	if err != nil {
		t.Fatalf("incremental export: %v", err)
	}
	if st2.Rows != 1 {
		t.Fatalf("incremental should export exactly the one changed row, got %d (skipped %d)", st2.Rows, st2.Skipped)
	}

	man1, err := ReadManifest(repo)
	if err != nil {
		t.Fatalf("read merged manifest: %v", err)
	}
	if len(man1.Files) != 2 {
		t.Fatalf("incremental should append a file, manifest has %d", len(man1.Files))
	}
	if man1.Watermark <= man0.Watermark {
		t.Fatalf("watermark should advance: base %d, merged %d", man0.Watermark, man1.Watermark)
	}
	// The merged manifest row total counts every published row, base plus the delta.
	if man1.Rows != int64(len(base)+1) {
		t.Fatalf("merged rows = %d, want %d", man1.Rows, len(base)+1)
	}

	// Import the whole repo: the overlapping key must be the newer copy, the rest
	// unchanged, and no duplicate rows.
	fileB := filepath.Join(dir, "b.meguri")
	res, err := Import(repo, fileB, ImportOptions{})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.URLCount != len(base) {
		t.Fatalf("import produced %d urls, want %d (dedup failed)", res.URLCount, len(base))
	}
	eng, err := live.Open(fileB)
	if err != nil {
		t.Fatalf("open imported: %v", err)
	}
	defer func() { _ = eng.Close() }()
	rec, ok, err := eng.GetURL(target)
	if err != nil || !ok {
		t.Fatalf("target key missing after import: ok=%v err=%v", ok, err)
	}
	if rec.ChangeCount != 99 || rec.CrawlCount != 100 || rec.LastChanged != 9000 {
		t.Fatalf("imported target is the stale copy: %+v", rec)
	}
}
