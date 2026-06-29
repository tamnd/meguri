package distribute

import (
	"sync"
	"sync/atomic"

	m "github.com/tamnd/meguri"
)

// Router maps a discovery's HostKey to the partition that owns it and ships the
// remote ones over the transport (doc 12, section 6, and doc 04). It caches a
// map snapshot and swaps in a newer one on a heartbeat, so routing is a
// lock-free map load plus the jump-hash arithmetic, with no per-discovery call
// to the control plane.
//
// The destination batcher is persistent, not per-call: it accumulates one
// partition's links across many outcomes and flushes a destination only when its
// batch fills, so links from a page's outcome and the next page's outcome to the
// same owner coalesce into one message instead of one per outcome. The engine
// calls Flush at the window edge (when it would otherwise idle) to ship the
// partials the fill threshold left behind.
type Router struct {
	self PartitionID
	cur  atomic.Pointer[Map]
	tr   Transport
	size int // batch size before a destination flushes

	mu  sync.Mutex // guards bat, since add and flushAll both mutate its pending map
	bat *batcher
}

// NewRouter builds a router for one partition over an initial map and transport.
// batchSize is how many discoveries accumulate for a destination before the
// router ships them as one message; a page's links to one partition coalesce
// into far fewer messages than links.
func NewRouter(self PartitionID, init *Map, tr Transport, batchSize int) *Router {
	r := &Router{self: self, tr: tr, size: batchSize, bat: newBatcher(tr, batchSize)}
	r.cur.Store(init)
	return r
}

// Map returns the router's current cached map snapshot.
func (r *Router) Map() *Map { return r.cur.Load() }

// Owner returns the partition that owns a HostKey under the cached map.
func (r *Router) Owner(hostKey uint64) PartitionID { return r.cur.Load().Owner(hostKey) }

// Local reports whether a discovery's host is owned by this partition, so the
// caller feeds it straight to the local dedup instead of the transport.
func (r *Router) Local(d m.Discovery) bool {
	return r.cur.Load().Owner(d.URLKey.HostKey) == r.self
}

// SwapMap installs a newer map, the heartbeat-pull convergence: the control
// plane bumps the epoch on a change, the partition fetches the new map, and the
// router swaps it in for its next routing decision. A stale or equal epoch is
// ignored so an out-of-order fetch never moves the router backward. It reports
// whether the swap happened.
func (r *Router) SwapMap(next *Map) bool {
	for {
		cur := r.cur.Load()
		if next.Epoch <= cur.Epoch {
			return false
		}
		if r.cur.CompareAndSwap(cur, next) {
			return true
		}
	}
}

// RouteLinks classifies one outcome's out-links: it returns the links whose
// hosts this partition owns for the caller's own intake, and adds every remote
// link to its owner's persistent batch, which ships as one message when it fills
// (doc 12, section 6). The batch outlives the call, so links from this outcome
// coalesce with links from later outcomes to the same owner; the engine calls
// Flush at the window edge to ship whatever stays below the fill threshold. A
// stale map only ever costs an extra hop: a link routed to a partition that no
// longer owns the host re-routes onward, and the receiver's seen-set dedups any
// redelivery, so RouteLinks never needs an exactly-correct map.
func (r *Router) RouteLinks(links []m.Discovery) (local []m.Discovery, err error) {
	mp := r.cur.Load()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range links {
		owner := mp.Owner(d.URLKey.HostKey)
		if owner == r.self {
			local = append(local, d)
			continue
		}
		if err = r.bat.add(owner, d); err != nil {
			return local, err
		}
	}
	return local, nil
}

// Flush ships every destination batch the router has accumulated below the
// per-destination fill size, the across-outcome counterpart to the per-fill
// flush inside add. The engine calls it when it would otherwise idle, so a
// partial batch the busy path left behind never waits past the point the
// partition quiesces. A send error is returned for the caller to retry; the
// discovery is rediscoverable either way.
func (r *Router) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bat.flushAll()
}

// Drain reads everything the transport has queued for this partition and returns
// it as one slice, the inbound discoveries the caller dedups and schedules. It
// is the receiver half of the transport: at-least-once means a discovery may
// appear more than once across drains, which the seen-set absorbs.
func (r *Router) Drain() []m.Discovery {
	var in []m.Discovery
	for {
		batch, ok := r.tr.Recv(r.self)
		if !ok {
			return in
		}
		in = append(in, batch...)
	}
}
