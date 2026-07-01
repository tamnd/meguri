package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/engine"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// runCmd executes a freshly built subcommand with args and returns its captured
// stdout, the small harness the CLI gates share. Each test builds its own command
// so flag state never leaks between runs.
func runCmd(t *testing.T, cmd *cobra.Command, args ...string) string {
	t.Helper()
	var buf bytes.Buffer
	cmd.SetArgs(args)
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute %v: %v", args, err)
	}
	return buf.String()
}

// seedPartitionDir opens a fresh partition directory, seeds host's URLs, and
// closes it so a durable .meguri lands on disk, the fixture the stats and map
// gates read back. It returns the directory.
func seedPartitionDir(t *testing.T, host string, n int) string {
	t.Helper()
	dir := t.TempDir()
	p, err := engine.OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("open partition: %v", err)
	}
	for i := range n {
		p.Frontier().Seed("https://"+host+"/p/"+string(rune('a'+i)), host, 0.5, 0, 0, 10)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close partition: %v", err)
	}
	return dir
}

// seedScheduledDir builds a wheel-on partition directory whose URLs carry an
// explicit due hour, so the checkpoint serializes the durable schedule region and
// both the live and cold schedule reads have due work to find. It returns the dir.
func seedScheduledDir(t *testing.T, host string, n int, due uint32) string {
	t.Helper()
	dir := t.TempDir()
	p, err := engine.OpenPartition(dir, store.Options{}, frontier.WithStateMachine(), frontier.WithScheduleIndex())
	if err != nil {
		t.Fatalf("open partition: %v", err)
	}
	for i := range n {
		p.Frontier().Seed("https://"+host+"/p/"+string(rune('a'+i)), host, 0.5, 0, due, 10)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close partition: %v", err)
	}
	return dir
}

// TestScheduleCommand gates the schedule command on both reads: live recovers the
// frontier and lists due URLs with their strings; cold reads the same partition's
// .meguri through the durable timing wheel, the predicate pushdown D13 wires in.
func TestScheduleCommand(t *testing.T) {
	dir := seedScheduledDir(t, "alpha.example", 4, 10)

	live := runCmd(t, newScheduleCmd(), "--data", dir, "--before", "100")
	if !strings.Contains(live, "(live)") {
		t.Fatalf("live schedule missing live marker:\n%s", live)
	}
	if !strings.Contains(live, "4 shown") {
		t.Fatalf("live schedule did not list the four due URLs:\n%s", live)
	}
	if !strings.Contains(live, "https://alpha.example/p/a") {
		t.Fatalf("live schedule missing a canonical URL:\n%s", live)
	}

	megFile := findMeguriFile(t, dir)
	cold := runCmd(t, newScheduleCmd(), "--data", megFile, "--before", "100")
	if !strings.Contains(cold, "durable schedule wheel") {
		t.Fatalf("cold schedule did not use the durable wheel pushdown:\n%s", cold)
	}
	if !strings.Contains(cold, "hour 100: 4") {
		t.Fatalf("cold schedule did not count the four due keys:\n%s", cold)
	}
}

// TestPackCommand gates the pack command: it writes a partition directory's live
// state to a fresh .meguri file, and the file reads back through the cold stats
// path with the same six seeded URLs. Packing must not mutate the source.
func TestPackCommand(t *testing.T) {
	dir := seedPartitionDir(t, "alpha.example", 6)
	out := filepath.Join(t.TempDir(), "snap.meguri")

	res := runCmd(t, newPackCmd(), "--data", dir, "--out", out)
	if !strings.Contains(res, "packed") || !strings.Contains(res, out) {
		t.Fatalf("pack did not report the output file:\n%s", res)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("pack output missing: %v", err)
	}

	cold := runCmd(t, newStatsCmd(), "--data", out)
	if !strings.Contains(cold, "urls           6") {
		t.Fatalf("packed file did not carry the six urls:\n%s", cold)
	}
}

// packGoneAndLive builds a one-Gone-one-live .meguri file: it seeds two URLs,
// drives one to Gone with a 410 and one to Crawled with a 200, and packs the
// frontier to a file. It returns the file path, the fixture the --gc gate needs.
func packGoneAndLive(t *testing.T) string {
	t.Helper()
	f := frontier.New(1, 0, frontier.WithStateMachine())
	// Two hosts so the second dispatch is not blocked by the first host's polite
	// crawl delay: one URL goes to Gone on a 410, the other to Crawled on a 200.
	f.Seed("https://gone.example/p", "gone.example", 0.5, 0, 0, 10)
	f.Seed("https://live.example/p", "live.example", 0.5, 0, 0, 10)
	for _, status := range []uint16{410, 200} {
		req, ok := f.Dispatch(0)
		if !ok {
			t.Fatal("nothing dispatched")
		}
		f.Report(meguri.Outcome{URLKey: req.URLKey, HTTPStatus: status, FetchedAt: 0, ContentFP: req.URLKey.PathKey | 1}, 0)
	}
	blob, err := f.CheckpointBytes()
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	path := filepath.Join(t.TempDir(), "gc.meguri")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// TestCompactCommand gates the compact command both ways: merging two disjoint
// single-host files folds their URL counts into one file, and --gc on a file
// holding a Gone tombstone drops it while keeping the live row.
func TestCompactCommand(t *testing.T) {
	dirA := seedPartitionDir(t, "alpha.example", 4)
	dirB := seedPartitionDir(t, "bravo.example", 4)
	fileA := filepath.Join(t.TempDir(), "a.meguri")
	fileB := filepath.Join(t.TempDir(), "b.meguri")
	runCmd(t, newPackCmd(), "--data", dirA, "--out", fileA)
	runCmd(t, newPackCmd(), "--data", dirB, "--out", fileB)

	mergedOut := filepath.Join(t.TempDir(), "merged.meguri")
	merge := runCmd(t, newCompactCmd(), "--out", mergedOut, fileA, fileB)
	if !strings.Contains(merge, "2 file(s)") || !strings.Contains(merge, "8 urls") {
		t.Fatalf("compact merge did not fold the two files:\n%s", merge)
	}
	cold := runCmd(t, newStatsCmd(), "--data", mergedOut)
	if !strings.Contains(cold, "urls           8") {
		t.Fatalf("merged file did not carry all eight urls:\n%s", cold)
	}

	gcFile := packGoneAndLive(t)
	gcOut := filepath.Join(t.TempDir(), "gc-out.meguri")
	res := runCmd(t, newCompactCmd(), "--gc", "--out", gcOut, gcFile)
	if !strings.Contains(res, "1 urls") || !strings.Contains(res, "1 Gone dropped") {
		t.Fatalf("compact --gc did not drop the Gone tombstone:\n%s", res)
	}
}

// TestStatsCommandLiveAndCold gates the stats command on both inputs the spec
// names: a partition directory (recover the frontier, print the per-status
// distribution and seen-set occupancy) and a single .meguri file (print the footer
// summary without recovery). Both must report the same six seeded, scheduled URLs.
func TestStatsCommandLiveAndCold(t *testing.T) {
	dir := seedPartitionDir(t, "alpha.example", 6)

	live := runCmd(t, newStatsCmd(), "--data", dir)
	if !strings.Contains(live, "(live)") {
		t.Fatalf("live stats missing live marker:\n%s", live)
	}
	if !strings.Contains(live, "urls           6") {
		t.Fatalf("live stats missing url count:\n%s", live)
	}
	if !strings.Contains(live, "by status") || !strings.Contains(live, "scheduled") {
		t.Fatalf("live stats missing scheduled bucket:\n%s", live)
	}

	// The cold path reads the .meguri the partition wrote. Find it in the dir.
	megFile := findMeguriFile(t, dir)
	cold := runCmd(t, newStatsCmd(), "--data", megFile)
	if !strings.Contains(cold, "(cold, footer summary)") {
		t.Fatalf("cold stats missing cold marker:\n%s", cold)
	}
	if !strings.Contains(cold, "urls           6") {
		t.Fatalf("cold stats missing url count:\n%s", cold)
	}
}

// TestMapCommand gates the map command over a manifest built from two single-host
// partitions: it lists both partitions, reports a clean tiling, and routes a known
// host key to its owning partition.
func TestMapCommand(t *testing.T) {
	hostA, hostB := "alpha.example", "bravo.example"
	dirA := seedPartitionDir(t, hostA, 4)
	dirB := seedPartitionDir(t, hostB, 4)

	entries := make([]format.ManifestEntry, 0, 2)
	for _, dir := range []string{dirA, dirB} {
		p, err := engine.OpenPartition(dir, store.Options{})
		if err != nil {
			t.Fatalf("open %s: %v", dir, err)
		}
		e, err := p.ManifestEntry(0)
		if err != nil {
			t.Fatalf("manifest entry: %v", err)
		}
		entries = append(entries, e)
		if err := p.Abandon(); err != nil {
			t.Fatalf("abandon: %v", err)
		}
	}
	manPath := filepath.Join(t.TempDir(), "fleet.mgm")
	if err := os.WriteFile(manPath, format.EncodeManifest(format.BuildManifest(entries)), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	out := runCmd(t, newMapCmd(), "--manifest", manPath)
	if !strings.Contains(out, "2 partitions") {
		t.Fatalf("map missing partition count:\n%s", out)
	}
	if !strings.Contains(out, "total   8 urls") {
		t.Fatalf("map missing fleet total:\n%s", out)
	}

	keyA := meguri.HostKeyOf(hostA)
	routed := runCmd(t, newMapCmd(), "--manifest", manPath, "--host", hexKey(keyA))
	if !strings.Contains(routed, "-> partition") {
		t.Fatalf("map --host did not route:\n%s", routed)
	}
}

// TestDatasetCommand gates the dataset bridge end to end: pack a partition to a
// .meguri, export it both to a single .parquet and to a Hugging Face repo folder, then
// import each back and confirm the url count survives the round trip.
func TestDatasetCommand(t *testing.T) {
	dir := seedPartitionDir(t, "alpha.example", 6)
	src := filepath.Join(t.TempDir(), "src.meguri")
	runCmd(t, newPackCmd(), "--data", dir, "--out", src)

	// Single-file mode.
	pq := filepath.Join(t.TempDir(), "urls.parquet")
	exp := runCmd(t, newDatasetExportCmd(), "--src", src, "--out", pq, "--codec", "zstd")
	if !strings.Contains(exp, "rows        6") || !strings.Contains(exp, "single-file") {
		t.Fatalf("single-file export did not report six rows:\n%s", exp)
	}
	back := filepath.Join(t.TempDir(), "back.meguri")
	runCmd(t, newDatasetImportCmd(), "--in", pq, "--out", back)
	cold := runCmd(t, newStatsCmd(), "--data", back)
	if !strings.Contains(cold, "urls           6") {
		t.Fatalf("single-file import lost urls:\n%s", cold)
	}

	// Repo mode: the folder carries data files, a card, and a manifest.
	repo := filepath.Join(t.TempDir(), "repo")
	rexp := runCmd(t, newDatasetExportCmd(), "--src", src, "--out", repo, "--repo")
	if !strings.Contains(rexp, "(repo)") {
		t.Fatalf("repo export did not report repo shape:\n%s", rexp)
	}
	if _, err := os.Stat(filepath.Join(repo, "README.md")); err != nil {
		t.Fatalf("repo missing card: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repo, "manifest.json")); err != nil {
		t.Fatalf("repo missing manifest: %v", err)
	}
	rback := filepath.Join(t.TempDir(), "rback.meguri")
	runCmd(t, newDatasetImportCmd(), "--in", repo, "--out", rback)
	rcold := runCmd(t, newStatsCmd(), "--data", rback)
	if !strings.Contains(rcold, "urls           6") {
		t.Fatalf("repo import lost urls:\n%s", rcold)
	}
}

// findMeguriFile returns the first .meguri snapshot in a partition directory.
func findMeguriFile(t *testing.T, dir string) string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".meguri") {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("no .meguri file in %s", dir)
	return ""
}

// hexKey formats a host key the way the map command parses it back.
func hexKey(k uint64) string {
	const hexdigits = "0123456789abcdef"
	var b [18]byte
	b[0], b[1] = '0', 'x'
	for i := range 16 {
		b[17-i] = hexdigits[k&0xf]
		k >>= 4
	}
	return string(b[:])
}
