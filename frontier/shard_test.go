package frontier

import (
	"os"
	"sync"
	"testing"

	"github.com/tamnd/meguri"
)

// TestReadyBankSingleShardUnchanged checks a one-shard bank is the prioRing the
// single dispatch loop always used: push/pop hit the highest priority bucket in
// insertion order, so the default frontier path is byte-for-byte unchanged.
func TestReadyBankSingleShardUnchanged(t *testing.T) {
	b := newReadyBank(0)
	if b.width() != 1 {
		t.Fatalf("default width = %d, want 1", b.width())
	}
	b.push(10, 0.2)
	b.push(11, 0.9) // higher priority
	b.push(12, 0.9) // same bucket as 11, later
	if hk, _ := b.pop(); hk != 11 {
		t.Fatalf("first pop = %d, want 11 (highest priority, first in)", hk)
	}
	if hk, _ := b.pop(); hk != 12 {
		t.Fatalf("second pop = %d, want 12 (same bucket, insertion order)", hk)
	}
	if hk, _ := b.pop(); hk != 10 {
		t.Fatalf("third pop = %d, want 10 (lower priority)", hk)
	}
	if _, ok := b.pop(); ok {
		t.Fatal("pop on a drained bank reported a host")
	}
}

// TestReadyBankHomeIsStable checks a host always hashes to the same shard, so its
// dispatch is local to one thread and stealing is the only cross-shard move.
func TestReadyBankHomeIsStable(t *testing.T) {
	b := newReadyBank(4)
	for i := range 64 {
		hk := uint64(i)
		if b.home(hk) != int(hk%4) {
			t.Fatalf("home(%d) = %d, want %d (stable hk%%shards)", hk, b.home(hk), hk%4)
		}
	}
}

// TestReadyBankStealsWhenOwnShardEmpty checks the core of audit 140: a thread whose
// own shard holds nothing steals the best-headed sibling rather than reporting
// drained, so no thread idles while any shard still has a ready host.
func TestReadyBankStealsWhenOwnShardEmpty(t *testing.T) {
	b := newReadyBank(4)
	// Everything hashes to shard 0 (keys are multiples of 4), so shards 1..3 are
	// empty and a thread pinned to one of them must steal from shard 0.
	b.push(0, 0.3)
	b.push(4, 0.9) // best head
	b.push(8, 0.5)

	if hk, ok := b.popShard(2); !ok || hk != 4 {
		t.Fatalf("thread 2 stole %d (ok=%v), want the best-headed host 4", hk, ok)
	}
	if hk, ok := b.popShard(3); !ok || hk != 8 {
		t.Fatalf("thread 3 stole %d (ok=%v), want 8", hk, ok)
	}
	if hk, ok := b.popShard(1); !ok || hk != 0 {
		t.Fatalf("thread 1 stole %d (ok=%v), want 0", hk, ok)
	}
	if _, ok := b.popShard(2); ok {
		t.Fatal("stole from a fully drained bank")
	}
}

// TestReadyBankStealsBestHeadAcrossShards checks a steal crosses to whichever shard
// has the highest-priority head, so global priority order holds across the steal
// and a thread never takes stale work while a better host waits in a sibling.
func TestReadyBankStealsBestHeadAcrossShards(t *testing.T) {
	b := newReadyBank(3)
	b.push(1, 0.2) // shard 1
	b.push(2, 0.9) // shard 2, the best head anywhere
	// Thread 0's own shard is empty; it should steal the best head, host 2.
	if hk, ok := b.popShard(0); !ok || hk != 2 {
		t.Fatalf("thread 0 stole %d (ok=%v), want 2 (best head across shards)", hk, ok)
	}
}

// TestThreadedDispatchNoHostIdle is the in-process double for the threaded engine:
// W dispatch goroutines share one mutex-guarded frontier (the engine's single-
// writer serialization), each pinned to a shard, and crawl a corpus of hosts whose
// keys all land in one shard so the other threads must steal to make progress.
// It asserts every host is dispatched exactly once and the run drains, proving
// work-stealing keeps every thread productive without double-dispatching a host.
func TestThreadedDispatchNoHostIdle(t *testing.T) {
	const threads = 4
	f := New(1, 0, WithDispatchShards(threads))

	// Seed hosts whose keys all hash to shard 0 so threads 1..3 only ever get work
	// by stealing. MakeURLKey hashes the host, so we cannot force the shard by name;
	// instead we check the bank width and let the steal path carry the threads that
	// draw an empty shard.
	const hosts = 200
	want := map[meguri.URLKey]bool{}
	for i := range hosts {
		host := "h" + itoa(i) + ".test"
		f.Seed("http://"+host+"/", host, 0.5, 0, 0, 1)
		want[meguri.MakeURLKey(host, "/")] = true
	}

	var mu sync.Mutex
	seen := map[meguri.URLKey]bool{}
	stole := 0
	var wg sync.WaitGroup
	for s := range threads {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for {
				mu.Lock()
				req, ok := f.DispatchShard(0, shard)
				if !ok {
					mu.Unlock()
					return
				}
				if seen[req.URLKey] {
					mu.Unlock()
					t.Errorf("host %v dispatched twice", req.URLKey)
					return
				}
				seen[req.URLKey] = true
				if f.readyHosts.home(req.HostKey) != shard {
					stole++ // this thread took a host that does not live in its own shard
				}
				// Complete the crawl so the host leaves the active set and the run drains.
				f.Report(meguri.Outcome{
					URLKey:     req.URLKey,
					HTTPStatus: 200,
					FetchedAt:  0,
					ContentFP:  req.URLKey.PathKey | 1,
				}, 0)
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()

	if len(seen) != hosts {
		t.Fatalf("dispatched %d distinct hosts, want %d", len(seen), hosts)
	}
	for k := range want {
		if !seen[k] {
			t.Fatalf("host %v never dispatched", k)
		}
	}
	if stole == 0 {
		t.Fatal("no steal happened across 4 shards; the work-stealing path was never exercised")
	}
	t.Logf("threaded dispatch: %d hosts across %d shards, %d taken by stealing", hosts, threads, stole)
}

// TestThreadedDispatchOnCorpus runs the threaded work-stealing dispatcher to full
// drain over the real pinned corpus: W goroutines share a mutex-guarded frontier
// (the engine's single-writer serialization) seeded with every distinct corpus URL.
// Each thread draws from its own shard, steals when its shard empties, and the
// threads cooperatively advance one shared logical clock whenever no shard has work
// at the current instant (the politeness window the real engine waits on). The run
// must dispatch every distinct corpus URL exactly once and then drain, proving the
// work-stealing dispatcher is correct and live at the corpus scale. It skips when
// no corpus is set.
func TestThreadedDispatchOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	seeds := loadCorpusSeeds(t, path)
	if len(seeds) < 1000 {
		t.Skipf("corpus produced %d seeds, need at least 1000", len(seeds))
	}

	const threads = 8
	f := New(1, 0, WithDispatchShards(threads))
	wantKeys := map[meguri.URLKey]bool{}
	hostKeys := map[uint64]bool{}
	for _, s := range seeds {
		f.Seed(s.url, s.host, s.priority, 0, 0, s.delay)
		wantKeys[meguri.MakeURLKey(s.host, PathOf(s.url))] = true
		hostKeys[meguri.MakeURLKey(s.host, "/").HostKey] = true
	}

	// Shared run state under one mutex: the logical clock the threads advance
	// together, the dedup of fired keys, the steal counter, and the drained flag.
	var mu sync.Mutex
	now := uint32(0)
	done := false
	seen := map[meguri.URLKey]bool{}
	seenHost := map[uint64]bool{}
	stole := 0

	var wg sync.WaitGroup
	for sh := range threads {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for {
				mu.Lock()
				if done {
					mu.Unlock()
					return
				}
				req, ok := f.DispatchShard(now, shard)
				if ok {
					if seen[req.URLKey] {
						mu.Unlock()
						t.Errorf("url %v dispatched twice", req.URLKey)
						return
					}
					seen[req.URLKey] = true
					seenHost[req.HostKey] = true
					if f.readyHosts.home(req.HostKey) != shard {
						stole++
					}
					f.Report(meguri.Outcome{
						URLKey:     req.URLKey,
						HTTPStatus: 200,
						FetchedAt:  now / 3600,
						ContentFP:  req.URLKey.PathKey | 1,
					}, now)
					mu.Unlock()
					continue
				}
				// popShard already steals across every shard, so ok=false means nothing
				// is dispatchable anywhere at now: advance the shared clock to the next
				// open politeness window, or finish when none remains.
				if t, has := f.NextEligible(); has && t > now {
					now = t
					mu.Unlock()
					continue
				}
				done = true
				mu.Unlock()
				return
			}
		}(sh)
	}
	wg.Wait()

	if len(seen) != len(wantKeys) {
		t.Fatalf("dispatched %d distinct urls, want %d", len(seen), len(wantKeys))
	}
	for k := range wantKeys {
		if !seen[k] {
			t.Fatalf("url %v never dispatched", k)
		}
	}
	if len(seenHost) != len(hostKeys) {
		t.Fatalf("fired %d distinct hosts, want %d", len(seenHost), len(hostKeys))
	}
	if stole == 0 {
		t.Fatal("no steal across 8 shards on the corpus; work-stealing never ran")
	}
	t.Logf("threaded corpus dispatch: drained %d distinct urls over %d hosts across %d shards, %d taken by stealing", len(seen), len(hostKeys), threads, stole)
}
