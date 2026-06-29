package dns

import (
	"context"
	"net"
	"sync"
	"time"
)

// DefaultTTL is the lifetime SystemResolver reports for a record. The Go
// resolver does not surface the DNS TTL, so we report a sane default and let the
// cache clamp it into [MinTTL, MaxTTL].
const DefaultTTL = 5 * time.Minute

// Default cache tuning, overridable with the Option functions.
const (
	defaultWorkers     = 16
	defaultMinTTL      = 60 * time.Second
	defaultMaxTTL      = 24 * time.Hour
	defaultNegativeTTL = 5 * time.Minute
)

// Resolver resolves a host name to an IP and the record's TTL. The production
// implementation is SystemResolver over the pure-Go net resolver; tests pass a
// stub. It must be safe for concurrent use.
type Resolver interface {
	Resolve(ctx context.Context, host string) (ip [16]byte, ttl time.Duration, err error)
}

// SystemResolver resolves over &net.Resolver{PreferGo: true}. Note: the Go
// resolver does not surface the record TTL, so Resolve returns DefaultTTL; the
// cache clamps TTLs into [MinTTL, MaxTTL] regardless.
type SystemResolver struct {
	// resolver is the underlying net resolver. A zero SystemResolver uses a
	// fresh pure-Go resolver, so the zero value is ready to use.
	resolver *net.Resolver
}

// Resolve looks up host and returns its first IP, IPv4-mapped into 16 bytes.
// It always uses the pure-Go resolver so a lookup costs a goroutine, not an OS
// thread.
func (s SystemResolver) Resolve(ctx context.Context, host string) ([16]byte, time.Duration, error) {
	r := s.resolver
	if r == nil {
		r = &net.Resolver{PreferGo: true}
	}
	addrs, err := r.LookupIPAddr(ctx, host)
	if err != nil {
		return [16]byte{}, 0, err
	}
	if len(addrs) == 0 {
		return [16]byte{}, 0, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
	}
	return ipToArray(addrs[0].IP), DefaultTTL, nil
}

// config holds the resolved Option values for a Cache.
type config struct {
	workers     int
	minTTL      time.Duration
	maxTTL      time.Duration
	negativeTTL time.Duration
}

// Option configures a Cache.
type Option func(*config)

// WithWorkers sets the prefetch pool concurrency. A value below 1 is ignored so
// the pool always has at least one worker.
func WithWorkers(n int) Option {
	return func(c *config) {
		if n >= 1 {
			c.workers = n
		}
	}
}

// WithMinTTL floors a tiny TTL so a short-lived record does not churn the cache.
func WithMinTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.minTTL = d
		}
	}
}

// WithMaxTTL caps a huge TTL so the cache eventually re-resolves.
func WithMaxTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.maxTTL = d
		}
	}
}

// WithNegativeTTL sets how long a failed name is suppressed before a retry.
func WithNegativeTTL(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.negativeTTL = d
		}
	}
}

// entry is one positive cache record: the resolved IP and its expiry instant.
type entry struct {
	ip     [16]byte
	expiry time.Time
}

// negEntry is one negative cache record: when the suppression lifts and how many
// times in a row the host has failed.
type negEntry struct {
	until    time.Time
	failures int
}

// Cache is the resolve cache: a positive cache (host -> ip, expiry) and a
// negative cache (host -> fail-until), refreshed by a bounded prefetch pool.
type Cache struct {
	resolver Resolver
	now      func() time.Time
	cfg      config

	mu       sync.Mutex
	positive map[string]entry
	negative map[string]negEntry
	inflight map[string]struct{} // hosts being resolved right now, for dedup

	queue   chan string
	wg      sync.WaitGroup // worker lifetimes
	pending sync.WaitGroup // queued-but-not-yet-finished work, for Wait

	closeOnce sync.Once
}

// NewCache builds a cache over r. now is an injectable clock for tests: pass a
// func returning the current time; nil means time.Now.
func NewCache(r Resolver, now func() time.Time, opts ...Option) *Cache {
	if now == nil {
		now = time.Now
	}
	cfg := config{
		workers:     defaultWorkers,
		minTTL:      defaultMinTTL,
		maxTTL:      defaultMaxTTL,
		negativeTTL: defaultNegativeTTL,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.minTTL > cfg.maxTTL {
		cfg.maxTTL = cfg.minTTL
	}

	c := &Cache{
		resolver: r,
		now:      now,
		cfg:      cfg,
		positive: make(map[string]entry),
		negative: make(map[string]negEntry),
		inflight: make(map[string]struct{}),
		// A buffer wide enough that a normal Prefetch burst does not block the
		// dispatcher. A full queue makes Prefetch drop the host; the next pass
		// re-queues it, which is fine since Prefetch is best-effort.
		queue: make(chan string, 1024),
	}
	c.wg.Add(cfg.workers)
	for i := 0; i < cfg.workers; i++ {
		go c.worker()
	}
	return c
}

// worker pulls hosts off the queue and resolves them until the queue closes.
func (c *Cache) worker() {
	defer c.wg.Done()
	for host := range c.queue {
		c.resolveAndStore(context.Background(), host)
		c.pending.Done()
	}
}

// Lookup returns the cached IP for host when present and unexpired. ok is false
// on a miss or an expired entry (the caller should Prefetch and defer the host,
// never resolve synchronously on the dispatch path).
func (c *Cache) Lookup(host string) (ip [16]byte, ok bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.positive[host]
	if !found || !now.Before(e.expiry) {
		return [16]byte{}, false
	}
	return e.ip, true
}

// Prefetch enqueues hosts for asynchronous resolution off the dispatch path.
// Hosts already fresh in the positive cache or still suppressed by the negative
// cache are skipped. It returns immediately; resolution happens in the pool.
func (c *Cache) Prefetch(hosts ...string) {
	now := c.now()
	for _, host := range hosts {
		if host == "" {
			continue
		}
		c.mu.Lock()
		if c.skipLocked(host, now) {
			c.mu.Unlock()
			continue
		}
		// Reserve the host so a concurrent Prefetch of the same name does not
		// queue it twice and so the worker pool resolves it only once.
		c.inflight[host] = struct{}{}
		c.mu.Unlock()

		c.pending.Add(1)
		select {
		case c.queue <- host:
		default:
			// Queue is full. Release the reservation and let a later pass try
			// again rather than block the dispatcher here.
			c.pending.Done()
			c.mu.Lock()
			delete(c.inflight, host)
			c.mu.Unlock()
		}
	}
}

// skipLocked reports whether host needs no work: it is fresh in the positive
// cache, still suppressed by the negative cache, or already in flight. The
// caller must hold c.mu.
func (c *Cache) skipLocked(host string, now time.Time) bool {
	if _, busy := c.inflight[host]; busy {
		return true
	}
	if e, ok := c.positive[host]; ok && now.Before(e.expiry) {
		return true
	}
	if n, ok := c.negative[host]; ok && now.Before(n.until) {
		return true
	}
	return false
}

// Resolve does a synchronous resolve-and-cache, for the rare path that needs it
// (and for tests). It honors the negative cache and the TTL clamps.
func (c *Cache) Resolve(ctx context.Context, host string) (ip [16]byte, ok bool) {
	now := c.now()
	c.mu.Lock()
	if e, found := c.positive[host]; found && now.Before(e.expiry) {
		c.mu.Unlock()
		return e.ip, true
	}
	if n, found := c.negative[host]; found && now.Before(n.until) {
		c.mu.Unlock()
		return [16]byte{}, false
	}
	c.mu.Unlock()
	return c.resolveAndStore(ctx, host)
}

// resolveAndStore performs the actual lookup and updates the positive or
// negative cache. It clears any in-flight reservation for the host. ok is false
// when the lookup failed.
func (c *Cache) resolveAndStore(ctx context.Context, host string) (ip [16]byte, ok bool) {
	rip, ttl, err := c.resolver.Resolve(ctx, host)

	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, host)

	if err != nil {
		n := c.negative[host]
		n.failures++
		n.until = now.Add(c.cfg.negativeTTL)
		c.negative[host] = n
		return [16]byte{}, false
	}

	// A success clears any prior suppression.
	delete(c.negative, host)
	c.positive[host] = entry{ip: rip, expiry: now.Add(c.clamp(ttl))}
	return rip, true
}

// clamp bounds a TTL into [MinTTL, MaxTTL].
func (c *Cache) clamp(ttl time.Duration) time.Duration {
	if ttl < c.cfg.minTTL {
		return c.cfg.minTTL
	}
	if ttl > c.cfg.maxTTL {
		return c.cfg.maxTTL
	}
	return ttl
}

// Suppressed reports whether host is currently negatively cached (a recent
// failure), so the caller can flag a dead host after repeated suppression.
func (c *Cache) Suppressed(host string) bool {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.negative[host]
	return ok && now.Before(n.until)
}

// Failures returns the current consecutive failure count for host, zero when the
// host is not suppressed. A run of failures is the signal to flag a dead host.
func (c *Cache) Failures(host string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.negative[host].failures
}

// Stats reports cache state for the gate and tests.
type Stats struct{ Positive, Negative, Pending int }

// Stats snapshots the cache sizes. Pending counts hosts reserved for or under
// resolution right now.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Positive: len(c.positive),
		Negative: len(c.negative),
		Pending:  len(c.inflight),
	}
}

// Wait blocks until the prefetch pool has drained all queued work. For tests
// and graceful shutdown.
func (c *Cache) Wait() {
	c.pending.Wait()
}

// Close stops the worker pool. It is safe to call more than once.
func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		close(c.queue)
	})
	c.wg.Wait()
}

// DialContext returns a net.Dialer DialContext func that dials the cached IP
// for the request's host directly (the per-IP politeness bucket is keyed on
// exactly this IP), falling back to the system dial when the host is uncached.
// The hostname still goes in the Host header and TLS SNI upstream; this only
// rewrites the dial address to ip:port.
func (c *Cache) DialContext(base *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	if base == nil {
		base = &net.Dialer{}
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return base.DialContext(ctx, network, c.dialAddr(addr))
	}
}

// dialAddr rewrites a host:port dial address to ip:port when the host is in the
// positive cache, and returns it unchanged otherwise. A malformed address with
// no host:port split is also returned unchanged.
func (c *Cache) dialAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if ip, ok := c.Lookup(host); ok {
		return net.JoinHostPort(arrayToIP(ip).String(), port)
	}
	return addr
}

// ipToArray normalizes a net.IP into the 16-byte form meguri stores: an IPv4
// address is IPv4-mapped, an IPv6 address is kept as is, and an invalid IP is
// the zero array.
func ipToArray(ip net.IP) [16]byte {
	var a [16]byte
	if v16 := ip.To16(); v16 != nil {
		copy(a[:], v16)
	}
	return a
}

// arrayToIP turns the 16-byte form back into a net.IP. An IPv4-mapped address
// prints as dotted-quad and an IPv6 address as its usual form.
func arrayToIP(a [16]byte) net.IP {
	ip := make(net.IP, net.IPv6len)
	copy(ip, a[:])
	return ip
}
