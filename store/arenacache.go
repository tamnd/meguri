package store

// arenaCache is the resident cost of the spilled arena (spec 2072 doc 05 section
// 3): a bounded LRU of recently-read URL strings keyed by arena offset. The
// spilled arena keeps the canonical URL bytes on disk (the snapshot string region
// and the log frames where they already live) and reads them by offset; this
// cache is the only part that stays in RAM, and its size is the budget B_arena.
//
// The key is the arena offset because the record already holds it (its *Ref
// field) and it uniquely names the string, so a lookup never has to hash the
// string to find its slot (doc 05 section 3a). The value is the decoded URL, an
// immutable Go string that is cheap to share and, on eviction, just a dropped
// reference the GC reclaims.
//
// The LRU is array-backed rather than container/list: a slice of nodes with
// intrusive prev/next indices and a free list, plus one map from offset to node
// index. This matters for the residency invariant the redesign sells. A
// container/list LRU allocates a separate Element per entry (~56 B each, its own
// GC object) on top of the map bucket, so its real resident heap runs ~1.4x the
// summed string bytes and B_arena stops being an honest bound. The array backing
// is two allocations that grow amortized (the node slice and the map), so the
// per-entry overhead is small, fixed, and countable, and B_arena bounds real heap
// to within the measured arenaEntryOverhead.
type arenaCache struct {
	budget int64 // max resident bytes (B_arena); 0 disables caching
	used   int64 // current resident bytes, sum of entry costs

	nodes []lruNode        // node pool; index 0..len-1, reused via the free list
	byOff map[uint64]int32 // offset -> node index
	head  int32            // most-recently-used node index, -1 if empty
	tail  int32            // least-recently-used node index, -1 if empty
	free  int32            // head of the free-node list, -1 if none

	hits    uint64
	misses  uint64
	evicted uint64
}

// lruNode is one cached string and its offset key, with intrusive doubly-linked
// list indices into the node pool. prev/next are node indices (-1 = end), so the
// whole list lives in one slice with no per-node allocation.
type lruNode struct {
	off        uint64
	s          string
	prev, next int32
}

// arenaEntryOverhead is the counted per-entry resident cost beyond the string
// bytes: the lruNode (off 8 + string header 16 + two int32 indices 8 = 32 B) plus
// the map slot (key 8 + value 4, ~16 B amortized with bucket overhead). 48 B is
// the honest figure for the array-backed layout, validated against measured held
// heap (TestArenaResidencyMeasure) rather than the ~140 B container/list cost.
const arenaEntryOverhead = 48

func nodeCost(s string) int64 { return int64(len(s)) + arenaEntryOverhead }

// newArenaCache builds a cache bounded to budget bytes. A zero or negative budget
// means "do not cache": get always misses and put is a no-op, so the spilled
// arena degrades to a pure pread-per-read path (still correct, just cold). That
// is the explicit-residency floor doc 05 section 2d/3 wants: the operator can set
// B_arena to zero and pay one disk read per string with no hidden resident bytes.
func newArenaCache(budget int64) *arenaCache {
	return &arenaCache{
		budget: budget,
		byOff:  make(map[uint64]int32),
		head:   -1,
		tail:   -1,
		free:   -1,
	}
}

// get returns the cached string for off and whether it was present. A hit moves
// the entry to the front (most-recently-used), the standard LRU touch.
func (c *arenaCache) get(off uint64) (string, bool) {
	if c.budget <= 0 {
		c.misses++
		return "", false
	}
	i, ok := c.byOff[off]
	if !ok {
		c.misses++
		return "", false
	}
	c.moveToFront(i)
	c.hits++
	return c.nodes[i].s, true
}

// put inserts (off, s) at the front and evicts from the back until the resident
// bytes are within budget. A string larger than the whole budget is not cached
// (it would evict everything and still not fit), so a pathologically long URL is
// served by pread every time rather than thrashing the cache. Re-putting an
// offset already present refreshes its position without double-counting bytes.
func (c *arenaCache) put(off uint64, s string) {
	if c.budget <= 0 {
		return
	}
	if i, ok := c.byOff[off]; ok {
		c.moveToFront(i)
		return
	}
	cost := nodeCost(s)
	if cost > c.budget {
		return
	}
	for c.used+cost > c.budget && c.tail != -1 {
		c.evictBack()
	}
	i := c.alloc()
	c.nodes[i] = lruNode{off: off, s: s, prev: -1, next: c.head}
	if c.head != -1 {
		c.nodes[c.head].prev = i
	}
	c.head = i
	if c.tail == -1 {
		c.tail = i
	}
	c.byOff[off] = i
	c.used += cost
}

// alloc returns a free node index, reusing an evicted slot from the free list or
// growing the pool. Growth is amortized append, so the pool is one slice that
// settles at the working-set size and is then reused without further allocation.
func (c *arenaCache) alloc() int32 {
	if c.free != -1 {
		i := c.free
		c.free = c.nodes[i].next
		return i
	}
	c.nodes = append(c.nodes, lruNode{})
	return int32(len(c.nodes) - 1)
}

// evictBack drops the least-recently-used entry and returns its slot to the free
// list. It is the inner step of put's eviction loop and the only place the cache
// shrinks.
func (c *arenaCache) evictBack() {
	i := c.tail
	if i == -1 {
		return
	}
	n := c.nodes[i]
	c.used -= nodeCost(n.s)
	delete(c.byOff, n.off)
	c.tail = n.prev
	if c.tail != -1 {
		c.nodes[c.tail].next = -1
	} else {
		c.head = -1
	}
	// Return the slot to the free list; clear the string so its bytes are
	// collectible and not pinned by the pooled node.
	c.nodes[i] = lruNode{prev: -1, next: c.free}
	c.free = i
	c.evicted++
}

// unlink removes node i from the list without freeing it, used by moveToFront.
func (c *arenaCache) unlink(i int32) {
	n := &c.nodes[i]
	if n.prev != -1 {
		c.nodes[n.prev].next = n.next
	} else {
		c.head = n.next
	}
	if n.next != -1 {
		c.nodes[n.next].prev = n.prev
	} else {
		c.tail = n.prev
	}
}

// moveToFront makes node i the most-recently-used.
func (c *arenaCache) moveToFront(i int32) {
	if c.head == i {
		return
	}
	c.unlink(i)
	c.nodes[i].prev = -1
	c.nodes[i].next = c.head
	if c.head != -1 {
		c.nodes[c.head].prev = i
	}
	c.head = i
	if c.tail == -1 {
		c.tail = i
	}
}

// drop clears the whole cache. The checkpoint calls this (doc 05 section 7b): a
// checkpoint rewrites the snapshot string region and remaps offsets, so the
// simplest correct choice is to drop the cache and let it re-warm from the new
// snapshot within a few scheduling bursts, rather than remap every key.
func (c *arenaCache) drop() {
	c.nodes = c.nodes[:0]
	c.byOff = make(map[uint64]int32)
	c.head = -1
	c.tail = -1
	c.free = -1
	c.used = 0
}

// stats reports the cache's resident bytes and hit accounting, the numbers the
// validation plan (doc 10) reads to confirm the realized hit rate and that the
// resident arena bytes equal B_arena, not O(N).
func (c *arenaCache) stats() (used, budget int64, hits, misses, evicted uint64) {
	return c.used, c.budget, c.hits, c.misses, c.evicted
}
