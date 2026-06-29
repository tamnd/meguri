package distribute

import (
	"sync"
	"sync/atomic"

	m "github.com/tamnd/meguri"
)

// SignalTransport carries a tsumugi import bundle from the producer (or the one
// partition that read the import file) to the partition that owns each host, the
// signal-side twin of Transport (doc 12, D16). It is one-way and at-least-once
// for the same reason: a bundle is never depended on and a newer epoch overwrites
// an older one, so a redelivered or dropped bundle is harmless. The two bindings
// are an in-process channel for one box and a partitioned log for a fleet.
type SignalTransport interface {
	// SendSignal delivers a bundle to one partition. The bundle holds only the
	// host and URL entries that partition owns, the per-destination split the
	// router computes.
	SendSignal(to PartitionID, s m.Signal) error
	// RecvSignal returns the next inbound bundle for self and true, or false when
	// nothing is queued.
	RecvSignal(self PartitionID) (m.Signal, bool)
}

// chanSignalTransport is the single-box binding: a bounded channel per
// destination, like chanTransport for discoveries. A crash loses an in-flight
// bundle, which is acceptable because the next import re-sends a superseding one.
type chanSignalTransport struct {
	mu    sync.Mutex
	chans map[PartitionID]chan m.Signal
	cap   int
}

// NewChannelSignalTransport builds the in-process signal transport whose
// per-destination channels buffer up to depth bundles before a SendSignal blocks.
func NewChannelSignalTransport(depth int) SignalTransport {
	if depth < 1 {
		depth = 1
	}
	return &chanSignalTransport{chans: map[PartitionID]chan m.Signal{}, cap: depth}
}

func (t *chanSignalTransport) chanFor(p PartitionID) chan m.Signal {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, ok := t.chans[p]
	if !ok {
		ch = make(chan m.Signal, t.cap)
		t.chans[p] = ch
	}
	return ch
}

func (t *chanSignalTransport) SendSignal(to PartitionID, s m.Signal) error {
	if len(s.Hosts) == 0 && len(s.URLs) == 0 {
		return nil
	}
	t.chanFor(to) <- s
	return nil
}

func (t *chanSignalTransport) RecvSignal(self PartitionID) (m.Signal, bool) {
	ch := t.chanFor(self)
	select {
	case s := <-ch:
		return s, true
	default:
		return m.Signal{}, false
	}
}

// SignalSink applies the entries of a bundle a partition owns. The prioritizer
// implements the URL half through ImportPageRank, and the engine fold writes the
// host half onto HostRecord.HostScore; a test binds a recording double. The
// router never calls the blend itself, it only delivers owned entries to the
// sink, so the policy stays in prioritize and the routing stays here.
type SignalSink interface {
	ImportURLSignal(m.URLSignal)
	ImportHostSignal(m.HostSignal)
}

// SignalRouter splits an import bundle by owning partition and ships each
// partition its own slice, then on the receive side applies inbound bundles to a
// sink under an epoch guard (doc 12, D16). It caches a map snapshot and swaps a
// newer one in on a heartbeat, the same lock-free read as the discovery router.
type SignalRouter struct {
	self PartitionID
	cur  atomic.Pointer[Map]
	tr   SignalTransport
	// seenEpoch is the highest bundle epoch this partition has applied. A bundle
	// with an epoch at or below it is dropped, so a redelivered or reordered
	// bundle never reverts a fresher import. It is never depended on for
	// correctness, only for not applying stale signal over fresh.
	seenEpoch uint64
}

// NewSignalRouter builds a signal router for one partition over an initial map
// and signal transport.
func NewSignalRouter(self PartitionID, init *Map, tr SignalTransport) *SignalRouter {
	r := &SignalRouter{self: self, tr: tr}
	r.cur.Store(init)
	return r
}

// Map returns the router's current cached map snapshot.
func (r *SignalRouter) Map() *Map { return r.cur.Load() }

// SwapMap installs a newer map, ignoring a stale or equal epoch, mirroring the
// discovery router so both move on the same heartbeat.
func (r *SignalRouter) SwapMap(next *Map) bool {
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

// RouteSignal splits a full import bundle by owning partition: every host and URL
// entry this partition owns is returned as the local bundle, and every remote
// entry is grouped by its owner and sent as one bundle per destination. The
// epoch rides every sub-bundle unchanged so each receiver applies the same
// version guard. A stale map only misroutes an entry, which the next import
// corrects, so the split never needs an exactly-correct map.
func (r *SignalRouter) RouteSignal(s m.Signal) (local m.Signal, err error) {
	mp := r.cur.Load()
	local.Epoch = s.Epoch
	out := map[PartitionID]*m.Signal{}
	dest := func(owner PartitionID) *m.Signal {
		if owner == r.self {
			return &local
		}
		b := out[owner]
		if b == nil {
			b = &m.Signal{Epoch: s.Epoch}
			out[owner] = b
		}
		return b
	}
	for _, h := range s.Hosts {
		b := dest(mp.Owner(h.HostKey))
		b.Hosts = append(b.Hosts, h)
	}
	for _, u := range s.URLs {
		b := dest(mp.Owner(u.URLKey.HostKey))
		b.URLs = append(b.URLs, u)
	}
	for owner, b := range out {
		if err = r.tr.SendSignal(owner, *b); err != nil {
			return local, err
		}
	}
	return local, nil
}

// Apply drains every inbound bundle and applies the entries to the sink, newest
// epoch winning. A bundle at or below the highest epoch already applied is
// dropped whole, so out-of-order delivery never reverts a fresher import. It
// returns the number of bundles applied, so a caller can tell whether an import
// landed this drain.
func (r *SignalRouter) Apply(sink SignalSink) int {
	applied := 0
	for {
		s, ok := r.tr.RecvSignal(r.self)
		if !ok {
			return applied
		}
		if s.Epoch <= r.seenEpoch {
			continue
		}
		r.seenEpoch = s.Epoch
		for _, h := range s.Hosts {
			sink.ImportHostSignal(h)
		}
		for _, u := range s.URLs {
			sink.ImportURLSignal(u)
		}
		applied++
	}
}

// ApplyLocal applies the local slice of a bundle this partition routed to itself,
// the same epoch guard as the inbound path so a self-routed bundle and a received
// one are imported identically.
func (r *SignalRouter) ApplyLocal(local m.Signal, sink SignalSink) bool {
	if local.Epoch <= r.seenEpoch || (len(local.Hosts) == 0 && len(local.URLs) == 0) {
		return false
	}
	r.seenEpoch = local.Epoch
	for _, h := range local.Hosts {
		sink.ImportHostSignal(h)
	}
	for _, u := range local.URLs {
		sink.ImportURLSignal(u)
	}
	return true
}
