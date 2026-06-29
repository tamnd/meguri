package frontier

// readyBank holds the hosts eligible to dispatch right now, each keyed by its best
// URL's priority. With one shard it is exactly the prioRing the single dispatch
// loop has always used, so the default path is byte-for-byte unchanged. With W
// shards it is the work-stealing ready bank of the threaded engine (audit 140, doc
// 05 [M1+]): each dispatch thread owns a shard, a host is hashed to a home shard
// for locality, and a thread whose own shard has drained steals the best-headed
// sibling shard rather than idling while another shard still holds ready work.
//
// The frontier stays single-writer: the engine serializes the W threads' Dispatch
// calls, so the sharding cuts where each thread looks for work, not the safety of
// the mutation. The win the structure models is liveness under W threads, no
// thread blocked behind a host bound to a different thread's shard.
type readyBank struct {
	shards []prioRing[uint64]
}

// newReadyBank builds a ready bank with w shards (w < 1 means the single-shard
// default). A host always returns to the same shard, so the shard count is fixed
// for the life of the frontier and a checkpoint replays into the same layout.
func newReadyBank(w int) readyBank {
	if w < 1 {
		w = 1
	}
	return readyBank{shards: make([]prioRing[uint64], w)}
}

// width reports how many shards the bank holds, the dispatch-thread count the
// frontier was built for.
func (b *readyBank) width() int { return len(b.shards) }

// home is the shard a host lives in, hashed off its key so a host always returns
// to the same shard. Stealing is then the only cross-shard move, which keeps a
// host's dispatch local to one thread in the common case and bounds how often two
// threads touch the same host's bucket.
func (b *readyBank) home(hostKey uint64) int {
	return int(hostKey % uint64(len(b.shards)))
}

// push files a ready host into its home shard at its best URL's priority.
func (b *readyBank) push(hostKey uint64, priority float32) {
	b.shards[b.home(hostKey)].push(hostKey, priority)
}

// pop is the single dispatch loop's path: the best ready host anywhere, taken as
// thread 0 with stealing, so a one-threaded caller drains every shard in priority
// order. With one shard it is the prioRing pop unchanged.
func (b *readyBank) pop() (uint64, bool) {
	return b.popShard(0)
}

// popShard serves dispatch thread s. It takes the best host from s's own shard, or,
// when that shard has drained, steals the best-headed host from the busiest sibling
// so no thread idles while any shard still holds a ready host. The steal picks the
// shard whose head sits in the highest priority bucket, so the stolen host is the
// best work available anywhere and global priority order is kept across the steal,
// ties broken by the lower shard index for a deterministic, replayable sequence.
func (b *readyBank) popShard(s int) (uint64, bool) {
	if s >= 0 && s < len(b.shards) {
		if hk, ok := b.shards[s].pop(); ok {
			return hk, true
		}
	}
	best, bestLvl := -1, -1
	for i := range b.shards {
		if lvl := b.shards[i].highest(); lvl > bestLvl {
			best, bestLvl = i, lvl
		}
	}
	if best < 0 {
		return 0, false
	}
	return b.shards[best].pop()
}
