package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// TestCheckpointStreamingMatchesMaterialized is the store-level byte-stability gate
// for the bounded checkpoint: the snapshot StreamEncodeToFile writes from the
// k-way shard merge must be byte-for-byte the snapshot the materializing
// snapshotPartition + EncodeToFile writes at the same page cap. If the merge ever
// reorders or drops a record, or the streamed columns drift, the bytes differ.
func TestCheckpointStreamingMatchesMaterialized(t *testing.T) {
	const maxPageRows = 64
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityNone})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 500
	for i := range n {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%13), fmt.Sprintf("/p%d", i), float32(i%7)))
	}
	for i := range 13 {
		s.PutHost(&meguri.HostRecord{HostKey: meguri.HostKeyOf(fmt.Sprintf("h%d.test", i)), URLBudget: uint32(64 + i)})
	}

	// Materialized reference at the same page cap.
	mat := s.Snapshot()
	mat.MaxPageRows = maxPageRows
	matPath := filepath.Join(dir, "materialized.meguri")
	if err := format.EncodeToFile(matPath, mat); err != nil {
		t.Fatalf("EncodeToFile: %v", err)
	}

	// Streamed snapshot from the shard merge.
	strPath := filepath.Join(dir, "streamed.meguri")
	if err := format.StreamEncodeToFile(strPath, newMergeSource(s), maxPageRows, s.snapshotShell(), dir); err != nil {
		t.Fatalf("StreamEncodeToFile: %v", err)
	}

	a, err := os.ReadFile(matPath)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(strPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("streamed snapshot differs from materialized (%d vs %d bytes)", len(a), len(b))
	}
}

// TestCheckpointStreamingRecoverIdentity is the end-to-end bounded checkpoint: fold
// the live store with CheckpointStreaming, reopen, and every record plus the arena
// must come back, the same contract TestCheckpointRecoverIdentity holds for the
// materializing path.
func TestCheckpointStreamingRecoverIdentity(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityNormal})
	if err != nil {
		t.Fatal(err)
	}
	const n = 400
	for i := range n {
		s.PutURL(mkURL(t, s, fmt.Sprintf("h%d.test", i%10), fmt.Sprintf("/p%d", i), float32(i)))
	}
	for i := range 10 {
		s.PutHost(&meguri.HostRecord{HostKey: meguri.HostKeyOf(fmt.Sprintf("h%d.test", i)), URLBudget: uint32(64 + i)})
	}
	if err := s.CheckpointStreaming(64); err != nil {
		t.Fatal(err)
	}
	// A post-checkpoint update lands in the fresh log and must replay on top.
	post := mkURL(t, s, "h3.test", "/p3", 99)
	post.CrawlCount = 7
	s.PutURL(post)
	s.Close()

	r, err := Open(dir, Options{Durability: DurabilityNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.URLCount() != n {
		t.Fatalf("recovered %d URLs, want %d", r.URLCount(), n)
	}
	if r.HostCount() != 10 {
		t.Fatalf("recovered %d hosts, want 10", r.HostCount())
	}
	got, ok := r.GetURL(meguri.MakeURLKey("h3.test", "/p3"))
	if !ok || got.Priority != 99 || got.CrawlCount != 7 {
		t.Fatalf("post-checkpoint update not replayed on top of the streamed snapshot: %+v", got)
	}
	if r.Str(got.URLRef) != "http://h3.test/p3" {
		t.Fatalf("arena not recovered across the streamed checkpoint: %q", r.Str(got.URLRef))
	}
	// Spot-check a record that was only in the snapshot, never re-logged.
	snapOnly, ok := r.GetURL(meguri.MakeURLKey("h7.test", "/p7"))
	if !ok || r.Str(snapOnly.URLRef) != "http://h7.test/p7" {
		t.Fatalf("snapshot-only record lost: ok=%v url=%q", ok, r.Str(snapOnly.URLRef))
	}
}
