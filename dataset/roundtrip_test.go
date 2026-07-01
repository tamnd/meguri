package dataset

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/live"
)

// sliceSource is an in-memory RecordSource of pre-sorted records for the round-trip.
type sliceSource struct {
	items []live.RecordItem
	i     int
}

func (s *sliceSource) Next() (live.RecordItem, bool, error) {
	if s.i >= len(s.items) {
		return live.RecordItem{}, false, nil
	}
	it := s.items[s.i]
	s.i++
	return it, true, nil
}

// sliceSink captures exported rows for comparison.
type sliceSink struct{ rows []Row }

func (s *sliceSink) write(row Row, _ *ExportStats) error { s.rows = append(s.rows, row); return nil }
func (s *sliceSink) close(_ *ExportStats) error          { return nil }
func (s *sliceSink) abort() error                        { return nil }
func (s *sliceSink) files() []FileMeta                   { return nil }

// readAllRows opens a .meguri and returns its rows in key order, the same projection
// an export produces, so two files compare row for row.
func readAllRows(t *testing.T, path string) []Row {
	t.Helper()
	eng, err := live.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = eng.Close() }()
	sink := &sliceSink{}
	var st ExportStats
	if err := exportReader(eng.Reader(), sink, 0, &st); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	return sink.rows
}

// sampleItems builds a small, field-varied, key-ordered record set across three hosts.
func sampleItems(t *testing.T) []live.RecordItem {
	t.Helper()
	mk := func(host, path string, mut func(*m.URLRecord)) live.RecordItem {
		key := m.MakeURLKey(host, path)
		rec := m.URLRecord{
			URLKey:          key,
			HostKey:         key.HostKey,
			Status:          m.StatusCrawled,
			Priority:        0.75,
			Depth:           3,
			DiscoverySource: m.SourceLink,
			FirstSeen:       100,
			NextDue:         500,
			LastCrawled:     420,
			LastChanged:     410,
			Lambda:          0.125,
			CrawlCount:      7,
			ChangeCount:     2,
			NoChangeStreak:  5,
			ContentFP:       0xdeadbeef,
			Simhash:         0x0badf00d,
			HTTPStatus:      200,
			RetryCount:      1,
			ErrorCount:      4,
		}
		if mut != nil {
			mut(&rec)
		}
		return live.RecordItem{Rec: rec, URL: "https://" + host + path, Host: host}
	}
	items := []live.RecordItem{
		mk("a.example", "/", func(r *m.URLRecord) { r.Status = m.StatusDiscovered; r.LastCrawled = 0; r.LastChanged = 0 }),
		mk("a.example", "/about", nil),
		mk("b.example", "/", func(r *m.URLRecord) { r.Depth = 0; r.DiscoverySource = m.SourceSeed }),
		mk("b.example", "/p?x=1", func(r *m.URLRecord) { r.Status = m.StatusGone; r.HTTPStatus = 410 }),
		mk("c.example", "/only", func(r *m.URLRecord) { r.LastModified = 300 }),
	}
	// ImportRecords needs key order.
	sortItems(items)
	return items
}

func sortItems(items []live.RecordItem) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].Rec.URLKey.Less(items[j-1].Rec.URLKey); j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}

func buildMeguri(t *testing.T, path string, items []live.RecordItem) {
	t.Helper()
	src := &sliceSource{items: items}
	if _, err := live.ImportRecords(src, live.BuildOptions{
		Path:         path,
		TmpDir:       filepath.Dir(path),
		ExpectedKeys: uint64(len(items)),
		PageRows:     2, // force multi-page so the paged read path is exercised
	}); err != nil {
		t.Fatalf("build %s: %v", path, err)
	}
}

// assertRowsEqual compares rows through their reconstructed records and strings. Row
// carries *time.Time pointer fields, so a struct == would compare pointers; the record
// FromRow rebuilds is all scalars (epoch-hours, not times), which is the lossless form
// the round-trip must preserve.
func assertRowsEqual(t *testing.T, want, got []Row) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("row count: want %d, got %d", len(want), len(got))
	}
	for i := range want {
		wr, wu, wh, we := FromRow(&want[i])
		gr, gu, gh, ge := FromRow(&got[i])
		if wr != gr || wu != gu || wh != gh || we != ge {
			t.Fatalf("row %d differs:\n want %+v url=%q host=%q etag=%q\n  got %+v url=%q host=%q etag=%q",
				i, wr, wu, wh, we, gr, gu, gh, ge)
		}
	}
}

// TestRoundTripRepo builds a .meguri, exports it to a repo folder, imports it back, and
// asserts every row survives the trip byte for byte.
func TestRoundTripRepo(t *testing.T) {
	dir := t.TempDir()
	fileA := filepath.Join(dir, "a.meguri")
	buildMeguri(t, fileA, sampleItems(t))
	wantRows := readAllRows(t, fileA)

	repo := filepath.Join(dir, "repo")
	st, err := ExportRepo(fileA, repo, ExportOptions{FileRows: 2, RowGroupRows: 2})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if st.Rows != int64(len(wantRows)) {
		t.Fatalf("exported %d rows, want %d", st.Rows, len(wantRows))
	}
	if st.Files < 2 {
		t.Fatalf("FileRows=2 over %d rows should split into multiple files, got %d", len(wantRows), st.Files)
	}

	// The card and manifest exist and the card carries the viewer frontmatter.
	card, err := os.ReadFile(filepath.Join(repo, CardName))
	if err != nil {
		t.Fatalf("read card: %v", err)
	}
	if !strings.Contains(string(card), "path: data/*.parquet") {
		t.Fatalf("card missing HF configs frontmatter:\n%s", card)
	}

	fileB := filepath.Join(dir, "b.meguri")
	if _, err := Import(repo, fileB, ImportOptions{PageRows: 2}); err != nil {
		t.Fatalf("import: %v", err)
	}
	assertRowsEqual(t, wantRows, readAllRows(t, fileB))
}

// TestRoundTripZeroTimestamps guards the fresh-seed case: a URL discovered but never
// dated or scheduled has every timestamp hour at zero. Those must survive as null
// columns that import back to zero, not wrap through an unrepresentable boundary date
// into a garbage hour (the bug where a non-optional zero time became 1754 in Parquet
// and hour 4293079695 on the way back).
func TestRoundTripZeroTimestamps(t *testing.T) {
	dir := t.TempDir()
	mk := func(host, path string) live.RecordItem {
		key := m.MakeURLKey(host, path)
		// Every time field left at the zero hour, the state a fresh seed produces.
		rec := m.URLRecord{
			URLKey:          key,
			HostKey:         key.HostKey,
			Status:          m.StatusDiscovered,
			DiscoverySource: m.SourceSeed,
			Priority:        0.5,
		}
		return live.RecordItem{Rec: rec, URL: "https://" + host + path, Host: host}
	}
	items := []live.RecordItem{mk("z.example", "/a"), mk("z.example", "/b"), mk("y.example", "/")}
	sortItems(items)

	fileA := filepath.Join(dir, "a.meguri")
	buildMeguri(t, fileA, items)
	wantRows := readAllRows(t, fileA)
	for i := range wantRows {
		if wantRows[i].FirstSeen != nil || wantRows[i].NextDue != nil {
			t.Fatalf("row %d: zero hour should export as a nil timestamp, got first_seen=%v next_due=%v",
				i, wantRows[i].FirstSeen, wantRows[i].NextDue)
		}
	}

	pq := filepath.Join(dir, "z.parquet")
	if _, err := ExportSingle(fileA, pq, ExportOptions{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	fileB := filepath.Join(dir, "b.meguri")
	if _, err := Import(pq, fileB, ImportOptions{}); err != nil {
		t.Fatalf("import: %v", err)
	}
	got := readAllRows(t, fileB)
	assertRowsEqual(t, wantRows, got)
	// The reconstructed hours must be zero, not a wrapped sentinel.
	for i := range got {
		rec, _, _, _ := FromRow(&got[i])
		if rec.FirstSeen != 0 || rec.NextDue != 0 {
			t.Fatalf("row %d imported non-zero hours from a null timestamp: first_seen=%d next_due=%d",
				i, rec.FirstSeen, rec.NextDue)
		}
	}
}

// TestRoundTripSingle checks the single-file export path round-trips too.
func TestRoundTripSingle(t *testing.T) {
	dir := t.TempDir()
	fileA := filepath.Join(dir, "a.meguri")
	buildMeguri(t, fileA, sampleItems(t))
	wantRows := readAllRows(t, fileA)

	pq := filepath.Join(dir, "one.parquet")
	if _, err := ExportSingle(fileA, pq, ExportOptions{}); err != nil {
		t.Fatalf("export single: %v", err)
	}
	fileB := filepath.Join(dir, "b.meguri")
	if _, err := Import(pq, fileB, ImportOptions{}); err != nil {
		t.Fatalf("import single: %v", err)
	}
	assertRowsEqual(t, wantRows, readAllRows(t, fileB))
}
