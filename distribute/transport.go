package distribute

import (
	"sync"

	m "github.com/tamnd/meguri"
)

// Transport carries discoveries from the partition that found a link to the
// partition that owns the link's host (doc 12, section 6). It is one-way and
// at-least-once: Send may be retried and the receiver may see a discovery more
// than once, and idempotency comes from the receiver's seen-set, not the
// transport, so the transport never needs a commit protocol. The two bindings
// are an in-process channel for one box and a partitioned log for a fleet; the
// router does not change a line between them.
type Transport interface {
	// Send delivers a batch of discoveries to the partition that owns their
	// hosts. The batch is keyed by destination, so all of one host's discoveries
	// land in the destination's one inbound stream. A full destination blocks the
	// producer, which is the backpressure that signals a rebalance, not a drop.
	Send(to PartitionID, batch []m.Discovery) error
	// Recv returns the next inbound batch for self and true, or false when the
	// transport is drained and closed. A partition's inbound queue is the union
	// of the streams for the hosts it owns.
	Recv(self PartitionID) ([]m.Discovery, bool)
}

// chanTransport is the single-box binding: a bounded Go channel per destination
// partition. A Send is a channel push, a Recv is a channel pop, and a crash
// loses the in-flight discoveries, which is acceptable because they are
// rediscoverable and the durable state is the live store, not the messages.
type chanTransport struct {
	mu    sync.Mutex
	chans map[PartitionID]chan []m.Discovery
	cap   int
}

// NewChannelTransport builds the in-process transport whose per-destination
// channels buffer up to depth batches before a Send blocks the producer. It is
// the single-box and test binding of Transport; a fleet binds a partitioned log
// behind the same interface.
func NewChannelTransport(depth int) Transport { return newChanTransport(depth) }

// newChanTransport builds an in-process transport whose per-destination channels
// buffer up to depth batches before a Send blocks the producer.
func newChanTransport(depth int) *chanTransport {
	if depth < 1 {
		depth = 1
	}
	return &chanTransport{chans: map[PartitionID]chan []m.Discovery{}, cap: depth}
}

// chanFor returns the inbound channel for a partition, creating it on first use
// so a Send to a not-yet-seen destination and a Recv from a quiet partition both
// work without a registration step.
func (t *chanTransport) chanFor(p PartitionID) chan []m.Discovery {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, ok := t.chans[p]
	if !ok {
		ch = make(chan []m.Discovery, t.cap)
		t.chans[p] = ch
	}
	return ch
}

func (t *chanTransport) Send(to PartitionID, batch []m.Discovery) error {
	if len(batch) == 0 {
		return nil
	}
	t.chanFor(to) <- batch
	return nil
}

// Recv pops one inbound batch without blocking; the second return is false when
// nothing is queued, so a caller polls its inbound stream and absorbs whatever
// has landed.
func (t *chanTransport) Recv(self PartitionID) ([]m.Discovery, bool) {
	ch := t.chanFor(self)
	select {
	case b := <-ch:
		return b, true
	default:
		return nil, false
	}
}

// batcher accumulates discoveries per destination and flushes a destination as
// one transport message when its batch fills or the caller flushes. It turns a
// per-link message rate into a per-destination message rate: a page with a
// hundred cross-partition links to thirty partitions is thirty messages, not a
// hundred (doc 12, section 6).
type batcher struct {
	tr      Transport
	maxSize int
	pending map[PartitionID][]m.Discovery
}

func newBatcher(tr Transport, maxSize int) *batcher {
	if maxSize < 1 {
		maxSize = 1
	}
	return &batcher{tr: tr, maxSize: maxSize, pending: map[PartitionID][]m.Discovery{}}
}

// add appends a discovery to a destination's batch and flushes that destination
// when it reaches maxSize, returning any send error so the producer can retry.
func (b *batcher) add(to PartitionID, d m.Discovery) error {
	b.pending[to] = append(b.pending[to], d)
	if len(b.pending[to]) >= b.maxSize {
		return b.flush(to)
	}
	return nil
}

// flush sends one destination's accumulated batch and clears it.
func (b *batcher) flush(to PartitionID) error {
	batch := b.pending[to]
	if len(batch) == 0 {
		return nil
	}
	delete(b.pending, to)
	return b.tr.Send(to, batch)
}

// flushAll sends every pending destination, the window-elapsed or end-of-outcome
// flush that ships partial batches so a discovery never waits unbounded.
func (b *batcher) flushAll() error {
	for to := range b.pending {
		if err := b.flush(to); err != nil {
			return err
		}
	}
	return nil
}
