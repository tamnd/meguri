package frontier

// waitItem is a host parked until its politeness window opens: the host key and
// the epoch-seconds at which it next becomes eligible to dispatch.
type waitItem struct {
	hostKey  uint64
	eligible uint32
}

// waitHeap is a binary min-heap of hosts keyed by eligible time (doc 05's host
// heap). When no host is ready now, the scheduler reads the root to learn the
// soonest moment a host opens up, and advances its clock straight to it rather
// than spinning. Ties on eligible time break by host key, so the dispatch order
// is fully determined and a recovered frontier reproduces it exactly.
type waitHeap struct {
	items []waitItem
}

func (h *waitHeap) less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if a.eligible != b.eligible {
		return a.eligible < b.eligible
	}
	return a.hostKey < b.hostKey
}

func (h *waitHeap) push(hostKey uint64, eligible uint32) {
	h.items = append(h.items, waitItem{hostKey: hostKey, eligible: eligible})
	h.up(len(h.items) - 1)
}

// peekMin returns the earliest-eligible host without removing it.
func (h *waitHeap) peekMin() (waitItem, bool) {
	if len(h.items) == 0 {
		return waitItem{}, false
	}
	return h.items[0], true
}

// popMin removes and returns the earliest-eligible host.
func (h *waitHeap) popMin() (waitItem, bool) {
	if len(h.items) == 0 {
		return waitItem{}, false
	}
	root := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	if len(h.items) > 0 {
		h.down(0)
	}
	return root, true
}

func (h *waitHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *waitHeap) down(i int) {
	n := len(h.items)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		small := left
		if right := left + 1; right < n && h.less(right, left) {
			small = right
		}
		if !h.less(small, i) {
			break
		}
		h.items[i], h.items[small] = h.items[small], h.items[i]
		i = small
	}
}
