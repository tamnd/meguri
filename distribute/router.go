package distribute

import (
	"sync/atomic"

	m "github.com/tamnd/meguri"
)

// Router maps a discovery's HostKey to the partition that owns it and ships the
// remote ones over the transport (doc 12, section 6, and doc 04). It caches a
// map snapshot and swaps in a newer one on a heartbeat, so routing is a
// lock-free map load plus the jump-hash arithmetic, with no per-discovery call
// to the control plane.
type Router struct {
	self PartitionID
	cur  atomic.Pointer[Map]
	tr   Transport
	size int // batch size before a destination flushes
}

// NewRouter builds a router for one partition over an initial map and transport.
// batchSize is how many discoveries accumulate for a destination before the
// router ships them as one message; a page's links to one partition coalesce
// into far fewer messages than links.
func NewRouter(self PartitionID, init *Map, tr Transport, batchSize int) *Router {
	r := &Router{self: self, tr: tr, size: batchSize}
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
// hosts this partition owns for the caller's own intake, and ships every remote
// link to its owner, batched by destination so the message count is per-owner,
// not per-link (doc 12, section 6). A stale map only ever costs an extra hop:
// a link routed to a partition that no longer owns the host re-routes onward,
// and the receiver's seen-set dedups any redelivery, so RouteLinks never needs
// an exactly-correct map.
func (r *Router) RouteLinks(links []m.Discovery) (local []m.Discovery, err error) {
	b := newBatcher(r.tr, r.size)
	mp := r.cur.Load()
	for _, d := range links {
		owner := mp.Owner(d.URLKey.HostKey)
		if owner == r.self {
			local = append(local, d)
			continue
		}
		if err = b.add(owner, d); err != nil {
			return local, err
		}
	}
	if err = b.flushAll(); err != nil {
		return local, err
	}
	return local, nil
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
