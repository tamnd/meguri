package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// TestPartitionLifecycleRoundTrip is the cheap no-corpus gate on the top-level
// open/close lifecycle: open a fresh directory, seed it, drive the engine to drain
// it, close (which checkpoints), then reopen and confirm the durable partition came
// back with every URL the session left behind. It proves the store-wrapping
// handle persists a frontier it recovered and advanced.
func TestPartitionLifecycleRoundTrip(t *testing.T) {
	dir := t.TempDir()

	p, err := OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fr := p.Frontier()
	hosts := []string{"a.example", "b.example", "c.example"}
	for _, h := range hosts {
		for i := range 5 {
			fr.Seed("https://"+h+"/p/"+string(rune('a'+i)), h, 0.5, 0, 0, 10)
		}
	}
	want := fr.Len()

	clk := NewLogicalClock(1_000_000)
	eng := New(fr, Config{Fetcher: &recFetcher{clk: clk}, Workers: 4, Clock: clk, UntilEmpty: true})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Abandon() }()
	if got := p2.Frontier().Len(); got != want {
		t.Fatalf("recovered %d urls, want %d", got, want)
	}
}

// TestPartitionAbandonKeepsLastCheckpoint confirms Abandon drops the session
// without persisting: an explicit Checkpoint commits a cut, a later seed is
// abandoned, and the reopened partition holds only the checkpointed URLs.
func TestPartitionAbandonKeepsLastCheckpoint(t *testing.T) {
	dir := t.TempDir()

	p, err := OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := range 4 {
		p.Frontier().Seed("https://kept.example/"+string(rune('a'+i)), "kept.example", 0.5, 0, 0, 10)
	}
	committed := p.Frontier().Len()
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Advance the in-memory frontier past the cut, then drop it without persisting.
	p.Frontier().Seed("https://dropped.example/x", "dropped.example", 0.5, 0, 0, 10)
	if err := p.Abandon(); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	p2, err := OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Abandon() }()
	if got := p2.Frontier().Len(); got != committed {
		t.Fatalf("recovered %d urls, want %d (abandoned seed must not persist)", got, committed)
	}
}

// TestCollectionRoutesToOwningPartition builds two single-host partitions, catalogs
// them in a manifest, writes and reopens it, and confirms the Manifest reader routes
// each host key to the partition that owns it and reopens that partition from its
// FileRef. It exercises the fleet-side reader the serve command holds.
func TestCollectionRoutesToOwningPartition(t *testing.T) {
	hostA, hostB := "alpha.example", "bravo.example"
	keyA, keyB := meguri.HostKeyOf(hostA), meguri.HostKeyOf(hostB)

	dirA := buildHostPartition(t, hostA)
	dirB := buildHostPartition(t, hostB)

	entries := make([]format.ManifestEntry, 0, 2)
	for _, dir := range []string{dirA, dirB} {
		p, err := OpenPartition(dir, store.Options{})
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

	col, err := OpenCollection(manPath)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	eA, ok := col.Route(keyA)
	if !ok || eA.FileRef != dirA {
		t.Fatalf("route(keyA) = %q,%v, want %q", eA.FileRef, ok, dirA)
	}
	eB, ok := col.Route(keyB)
	if !ok || eB.FileRef != dirB {
		t.Fatalf("route(keyB) = %q,%v, want %q", eB.FileRef, ok, dirB)
	}

	// The routed entry reopens to the partition that owns the host.
	p, err := col.OpenPartition(eA, store.Options{})
	if err != nil {
		t.Fatalf("open routed partition: %v", err)
	}
	defer func() { _ = p.Abandon() }()
	if got := p.Frontier().Len(); got == 0 {
		t.Fatal("routed partition recovered empty")
	}
}

// TestPartitionServeOnCorpus is the corpus gate on the durable lifecycle: open a
// fresh partition, seed the frozen CC-MAIN-2026-25 slice into it, drive the engine
// to drain it, close (checkpoint), then reopen and confirm every distinct URL
// survived the round trip through the log-structured store. This is the serve
// command's open/seed/advance/checkpoint/close path on real data.
func TestPartitionServeOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice")
	}
	dir := t.TempDir()

	p, err := OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seedFrontierFromCorpus(t, p.Frontier(), path)
	want := p.Frontier().Len()
	if want < 1000 {
		_ = p.Abandon()
		t.Skipf("corpus has %d urls, need at least 1000", want)
	}

	clk := NewLogicalClock(1_700_000_000)
	eng := New(p.Frontier(), Config{Fetcher: &recFetcher{clk: clk}, Workers: 16, Clock: clk, UntilEmpty: true})
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := OpenPartition(dir, store.Options{}, frontier.WithStateMachine())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Abandon() }()
	if got := p2.Frontier().Len(); got != want {
		t.Fatalf("recovered %d urls, want %d", got, want)
	}
	t.Logf("durable lifecycle round-tripped %d real urls through the store", want)
}

// buildHostPartition opens a fresh partition, seeds one host's URLs, and closes it,
// returning the directory. It is the fixture the manifest route test catalogs.
func buildHostPartition(t *testing.T, host string) string {
	t.Helper()
	dir := t.TempDir()
	p, err := OpenPartition(dir, store.Options{})
	if err != nil {
		t.Fatalf("open %s: %v", host, err)
	}
	for i := range 6 {
		p.Frontier().Seed("https://"+host+"/p/"+string(rune('a'+i)), host, 0.5, 0, 0, 10)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close %s: %v", host, err)
	}
	return dir
}

// seedFrontierFromCorpus loads a CDX JSONL slice into a frontier, the corpus intake
// the durable-lifecycle gate uses to fill a partition with real URLs.
func seedFrontierFromCorpus(t *testing.T, fr *frontier.Frontier, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open corpus: %v", err)
	}
	defer func() { _ = f.Close() }()
	type rec struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r rec
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		host := r.Host
		if host == "" {
			host = frontier.HostOf(r.URL)
		}
		if host == "" {
			continue
		}
		fr.Seed(r.URL, host, 0.5, 0, 0, 10)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan corpus: %v", err)
	}
}
