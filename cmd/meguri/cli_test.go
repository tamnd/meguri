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
