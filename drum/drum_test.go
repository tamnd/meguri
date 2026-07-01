package drum

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/meguri"
)

func key(h, p uint64) meguri.URLKey { return meguri.URLKey{HostKey: h, PathKey: p} }

// verdictMap collapses a verdict slice to key -> unique for assertions.
func verdictMap(vs []Classification) map[meguri.URLKey]bool {
	m := make(map[meguri.URLKey]bool, len(vs))
	for _, v := range vs {
		m[v.Key] = v.Unique
	}
	return m
}

func mustLocate(t *testing.T, d *DRUM, k meguri.URLKey) (int64, uint64, bool) {
	t.Helper()
	off, lsn, present, err := d.Locate(k)
	if err != nil {
		t.Fatalf("Locate(%v): %v", k, err)
	}
	return off, lsn, present
}

func TestDiscoverClassifiesUniqueAndDuplicate(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// First merge: three distinct keys, all unique.
	for i := uint64(1); i <= 3; i++ {
		if err := d.Discover(key(i, i*10), int64(i*100), i); err != nil {
			t.Fatal(err)
		}
	}
	vs, err := d.Merge()
	if err != nil {
		t.Fatal(err)
	}
	got := verdictMap(vs)
	for i := uint64(1); i <= 3; i++ {
		if !got[key(i, i*10)] {
			t.Fatalf("key %d should be unique on first sight", i)
		}
	}

	// Second merge: re-discover key 2 (duplicate) and add key 4 (unique).
	if err := d.Discover(key(2, 20), 250, 5); err != nil {
		t.Fatal(err)
	}
	if err := d.Discover(key(4, 40), 400, 6); err != nil {
		t.Fatal(err)
	}
	vs, err = d.Merge()
	if err != nil {
		t.Fatal(err)
	}
	got = verdictMap(vs)
	if got[key(2, 20)] {
		t.Fatal("re-discovered key 2 should be a duplicate")
	}
	if !got[key(4, 40)] {
		t.Fatal("key 4 should be unique on first sight")
	}
}

func TestLocatePointRead(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for i := uint64(1); i <= 1000; i++ {
		if err := d.Discover(key(i%97, i), int64(i*8), i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 1000; i++ {
		off, lsn, present := mustLocate(t, d, key(i%97, i))
		if !present {
			t.Fatalf("key %d absent after merge", i)
		}
		if off != int64(i*8) || lsn != i {
			t.Fatalf("key %d: got off=%d lsn=%d want off=%d lsn=%d", i, off, lsn, i*8, i)
		}
	}
	// A never-discovered key is absent.
	if _, _, present := mustLocate(t, d, key(5, 999999)); present {
		t.Fatal("unknown key should be absent")
	}
}

func TestLastWriterWinsRepoints(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	k := key(7, 70)
	if err := d.Discover(k, 100, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	off, lsn, _ := mustLocate(t, d, k)
	if off != 100 || lsn != 1 {
		t.Fatalf("after first put: off=%d lsn=%d", off, lsn)
	}

	// A crawl moved the record: newer lsn, new offset.
	if err := d.Discover(k, 500, 9); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	off, lsn, _ = mustLocate(t, d, k)
	if off != 500 || lsn != 9 {
		t.Fatalf("after relocation: off=%d lsn=%d, want off=500 lsn=9", off, lsn)
	}

	// A stale rediscovery (older lsn) must not regress the index.
	if err := d.Discover(k, 200, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	off, lsn, _ = mustLocate(t, d, k)
	if off != 500 || lsn != 9 {
		t.Fatalf("stale rediscovery regressed index: off=%d lsn=%d", off, lsn)
	}
}

func TestTombstoneRemoves(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	k := key(3, 33)
	if err := d.Discover(k, 100, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, _, present := mustLocate(t, d, k); !present {
		t.Fatal("key should be present before tombstone")
	}
	if err := d.Tombstone(k, 300, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	if _, _, present := mustLocate(t, d, k); present {
		t.Fatal("tombstoned key should be absent")
	}
}

func TestOverlayBeforeMerge(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// A high-priority seed: discovered but not yet merged. The overlay must find it.
	k := key(11, 110)
	if err := d.Discover(k, 808, 3); err != nil {
		t.Fatal(err)
	}
	off, lsn, present := mustLocate(t, d, k)
	if !present || off != 808 || lsn != 3 {
		t.Fatalf("overlay miss before merge: present=%v off=%d lsn=%d", present, off, lsn)
	}
	// After the merge it is served from the repository, same answer.
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	off, lsn, present = mustLocate(t, d, k)
	if !present || off != 808 || lsn != 3 {
		t.Fatalf("repo miss after merge: present=%v off=%d lsn=%d", present, off, lsn)
	}
}

func TestRecoverReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5000; i++ {
		if err := d.Discover(key(i%200, i), int64(i*16), i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the block index is loaded (or rebuilt) and every key is locatable.
	d2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	for i := uint64(1); i <= 5000; i++ {
		off, lsn, present := mustLocate(t, d2, key(i%200, i))
		if !present || off != int64(i*16) || lsn != i {
			t.Fatalf("after reopen, key %d: present=%v off=%d lsn=%d", i, present, off, lsn)
		}
	}

	// Deleting the index file forces a rebuild from the repository on the next Open.
	if err := os.Remove(filepath.Join(dir, "drum", repoIdxName)); err != nil {
		t.Fatal(err)
	}
	d3, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d3.Close()
	off, _, present := mustLocate(t, d3, key(123%200, 123))
	if !present || off != int64(123*16) {
		t.Fatalf("after index rebuild, key 123: present=%v off=%d", present, off)
	}
}

func TestTornPendingDiscardedOnOpen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Force a flushed (but unmerged) pending file by using a tiny flush threshold.
	d.flushBytes = pendRecordSize
	if err := d.Discover(key(1, 1), 10, 1); err != nil {
		t.Fatal(err)
	}
	if err := d.Discover(key(1, 2), 20, 2); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	names, err := listPending(filepath.Join(dir, "drum"))
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("expected a flushed pending file")
	}
	// Corrupt one pending file's body so its CRC fails.
	corrupt := names[0]
	b, err := os.ReadFile(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(corrupt, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// Reopen: the torn file is discarded, Open succeeds.
	d2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if _, err := os.Stat(corrupt); !os.IsNotExist(err) {
		t.Fatalf("torn pending file should have been removed: %v", err)
	}
}

// TestOnDiskCostPerURL pins the spec's central claim: the repository costs the
// fixed-width 29 bytes per distinct URL on disk (doc 04 section 3.2), and nothing
// resident scales per URL. It is the Stage B counterpart to the ladder's ~80 B/url
// resident index the redesign removes.
func TestOnDiskCostPerURL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const n = 100000
	for i := uint64(1); i <= n; i++ {
		if err := d.Discover(key(i, i*3+1), int64(i*32), i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Merge(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(d.repoPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(n)*repoRecordSize {
		t.Fatalf("repository = %d bytes, want %d (%d B/url * %d urls)", fi.Size(), int64(n)*repoRecordSize, repoRecordSize, n)
	}
	perURL := float64(fi.Size()) / n
	t.Logf("repository on-disk: %.1f B/url over %d urls (block index entries: %d, ~%.3f B/url)",
		perURL, n, len(d.bi.entries), float64(len(d.bi.entries)*idxEntrySize)/n)
}
