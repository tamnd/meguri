package frontier

import "github.com/tamnd/meguri"

// wheelSpan is the timing wheel's resident horizon in epoch-hours. A due time
// within the span of the cursor lands in a ring bucket; anything farther waits in
// the overflow heap and is pulled into the ring as the cursor advances. 1024
// hours is about 42 days, comfortably past the flat recrawl gap (168h) and every
// retry backoff, so in steady state the overflow heap stays empty and only a
// manually far-dated seed ever reaches it.
const wheelSpan = 1024

// dueWheel is the resident schedule index (doc 06, doc 05 near tier, M6): a
// hashed timing wheel over epoch-hours that defers a not-yet-due URL until its
// NextDue arrives, instead of leaving it in the front bank for the dispatch path
// to keep skipping. A crawled URL waiting out its recrawl interval and a
// future-dated seed both wait here; the cursor advances with the dispatch clock
// and fires each passed hour's bucket back into the schedule.
//
// The wheel is derived state, not durable state: a URL's due time and status live
// in its record, so Recover rebuilds the wheel from the URL table (a Crawled URL
// with a future NextDue is a pending recrawl) without the wheel ever being
// serialized. That is the durable form of the index doc 11 names: the store holds
// the URL and host tables the wheel is computed from.
type dueWheel struct {
	cursor    uint32                      // epoch-hour the wheel has advanced through; due <= cursor has fired
	ring      [wheelSpan][]meguri.URLKey  // hour buckets; a key due at hour h lives in ring[h%wheelSpan]
	over      overHeap                    // due >= cursor+wheelSpan, by due hour, pulled in as the cursor nears
	ringCount int                         // keys resident in the ring, so an empty ring is an O(1) check
}

// newDueWheel starts a wheel whose cursor sits at base epoch-hours, so a due time
// at or before base fires on the first advance.
func newDueWheel(base uint32) *dueWheel { return &dueWheel{cursor: base} }

// add files a URL to fire at dueHour. A due time at or before the cursor lands in
// the cursor's bucket and fires on the next advance; a time within the horizon
// lands in its ring bucket; a far-future one waits in the overflow heap until the
// cursor advances within a span of it.
func (w *dueWheel) add(key meguri.URLKey, dueHour uint32) {
	if dueHour <= w.cursor {
		dueHour = w.cursor
	}
	if dueHour-w.cursor < wheelSpan {
		i := dueHour % wheelSpan
		w.ring[i] = append(w.ring[i], key)
		w.ringCount++
		return
	}
	w.over.push(overItem{due: dueHour, key: key})
}

// due advances the cursor to nowHour and returns every key whose hour the cursor
// passed, the URLs now eligible to (re)enter the schedule. It admits overflow
// entries into the ring as the cursor nears them, and fast-forwards over an empty
// horizon so a far-future entry costs O(wheelSpan), never O(gap).
func (w *dueWheel) due(nowHour uint32) []meguri.URLKey {
	var fired []meguri.URLKey
	for w.cursor <= nowHour {
		if w.ringCount == 0 {
			it, ok := w.over.peek()
			if !ok {
				w.cursor = nowHour + 1 // nothing waiting: the wheel is caught up
				break
			}
			edge := satSub(it.due, wheelSpan-1) // hour the overflow head enters the ring
			if edge > nowHour {
				w.cursor = nowHour + 1 // even the earliest event is past the horizon we advance to
				break
			}
			if edge > w.cursor {
				w.cursor = edge
			}
			w.admitOverflow()
			continue
		}
		i := w.cursor % wheelSpan
		if b := w.ring[i]; len(b) > 0 {
			fired = append(fired, b...)
			w.ringCount -= len(b)
			w.ring[i] = nil
		}
		w.cursor++
		w.admitOverflow()
	}
	return fired
}

// admitOverflow moves every overflow entry now within the horizon into its ring
// bucket, the refill that runs each time the cursor advances.
func (w *dueWheel) admitOverflow() {
	for {
		it, ok := w.over.peek()
		if !ok {
			return
		}
		if it.due > w.cursor && it.due-w.cursor >= wheelSpan {
			return // the head is still beyond the horizon
		}
		w.over.pop()
		due := max(it.due, w.cursor) // an overdue straggler fires in the current bucket
		w.ring[due%wheelSpan] = append(w.ring[due%wheelSpan], it.key)
		w.ringCount++
	}
}

// nextDue returns the earliest hour the wheel will fire and whether anything is
// pending, so the scheduler can advance its clock to a due recrawl when no host is
// otherwise ready. The forward ring scan is bounded by the span, so it is O(1) in
// the frontier size.
func (w *dueWheel) nextDue() (uint32, bool) {
	if w.ringCount > 0 {
		for d := w.cursor; d < w.cursor+wheelSpan; d++ {
			if len(w.ring[d%wheelSpan]) > 0 {
				return d, true
			}
		}
	}
	if it, ok := w.over.peek(); ok {
		return it.due, true
	}
	return 0, false
}

// len reports how many URLs the wheel is holding for a future due time, across the
// ring and the overflow heap.
func (w *dueWheel) len() int { return w.ringCount + w.over.len() }

// satSub is a saturating subtraction on epoch-hours, so computing a horizon edge
// near hour zero never underflows.
func satSub(a, b uint32) uint32 {
	if a < b {
		return 0
	}
	return a - b
}

// overItem is one entry of the overflow heap: a URL keyed by the hour it is due.
type overItem struct {
	due uint32
	key meguri.URLKey
}

// overHeap is a min-heap of overflow entries by due hour, the far-future tier of
// the wheel. It is a hand-rolled binary heap to keep the keys unboxed; the
// overflow tier is rarely touched, so the simplicity is worth more than a tuned
// structure.
type overHeap struct{ a []overItem }

func (h *overHeap) len() int { return len(h.a) }

func (h *overHeap) peek() (overItem, bool) {
	if len(h.a) == 0 {
		return overItem{}, false
	}
	return h.a[0], true
}

func (h *overHeap) push(it overItem) {
	h.a = append(h.a, it)
	i := len(h.a) - 1
	for i > 0 {
		p := (i - 1) / 2
		if h.a[p].due <= h.a[i].due {
			break
		}
		h.a[p], h.a[i] = h.a[i], h.a[p]
		i = p
	}
}

func (h *overHeap) pop() (overItem, bool) {
	n := len(h.a)
	if n == 0 {
		return overItem{}, false
	}
	top := h.a[0]
	h.a[0] = h.a[n-1]
	h.a = h.a[:n-1]
	n--
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		small := i
		if l < n && h.a[l].due < h.a[small].due {
			small = l
		}
		if r < n && h.a[r].due < h.a[small].due {
			small = r
		}
		if small == i {
			break
		}
		h.a[small], h.a[i] = h.a[i], h.a[small]
		i = small
	}
	return top, true
}
