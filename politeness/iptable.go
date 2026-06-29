package politeness

import (
	"sync"
	"time"
)

// IPTable is the shared per-IP politeness state. Many host groups can resolve to
// one address (a CDN, shared hosting), and hammering that machine through a
// hundred vhosts is the rudeness the per-host bucket alone cannot catch. The
// table reduces each IP to a single next-eligible instant, advanced every time a
// host on that IP dispatches, and stays bounded by the in-flight IP working set
// because idle buckets are evicted.
//
// It is safe for concurrent use: the engine dispatches many hosts at once and
// several of them may share an address.
type IPTable struct {
	mu      sync.Mutex
	buckets map[[16]byte]ipBucket
	floor   time.Duration
}

// ipBucket is one IP's reduced bucket: when it may next be fetched, and when it
// was last touched so an idle address can be evicted.
type ipBucket struct {
	nextEligible int64 // unix seconds, earliest next fetch on this IP
	touched      int64 // unix seconds, last dispatch on this IP
}

// zeroIP is an unresolved host's address. It is never gated: per-host politeness
// is the only constraint until DNS lands the real address.
var zeroIP [16]byte

// NewIPTable returns an empty table whose per-IP interval never drops below
// floor, regardless of how fast an individual host on that IP is allowed to go.
func NewIPTable(floor time.Duration) *IPTable {
	return &IPTable{buckets: make(map[[16]byte]ipBucket), floor: floor}
}

// EligibleAt returns the unix-second instant ip may next be fetched, or 0 when
// the IP is unknown or unresolved (the zero address).
func (t *IPTable) EligibleAt(ip [16]byte) int64 {
	if ip == zeroIP {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buckets[ip].nextEligible
}

// Spend records a dispatch to ip at unix-second now using the host's interval,
// pushing the IP's next-eligible forward by at least the per-IP floor. A fetch
// is only dispatched when the IP already permits, so now is at or past the old
// next-eligible and the advance is monotonic. It is a no-op for the zero IP.
func (t *IPTable) Spend(ip [16]byte, now int64, interval time.Duration) {
	if ip == zeroIP {
		return
	}
	step := max(interval, t.floor)
	secs := max(int64(step/time.Second), 1)
	t.mu.Lock()
	t.buckets[ip] = ipBucket{nextEligible: now + secs, touched: now}
	t.mu.Unlock()
}

// Evict drops IP buckets untouched at or before cutoff, keeping the table sized
// to the active IP working set rather than every address ever resolved. It
// returns the number evicted.
func (t *IPTable) Evict(cutoff int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for ip, b := range t.buckets {
		if b.touched <= cutoff {
			delete(t.buckets, ip)
			n++
		}
	}
	return n
}

// Len reports the number of resident IP buckets, the table's memory footprint.
func (t *IPTable) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.buckets)
}
