package dns

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubResolver is a canned resolver for tests. It returns a fixed IP and TTL for
// known hosts, an error for hosts in fail, and counts every call atomically so a
// dedup test can assert the resolver ran exactly once.
type stubResolver struct {
	mu    sync.Mutex
	ips   map[string][16]byte
	ttls  map[string]time.Duration
	fail  map[string]bool
	calls int64
	// block, when set, holds every Resolve until released. It lets a test pin a
	// resolve in flight while it inspects the cache.
	block chan struct{}
}

func newStub() *stubResolver {
	return &stubResolver{
		ips:  map[string][16]byte{},
		ttls: map[string]time.Duration{},
		fail: map[string]bool{},
	}
}

func (s *stubResolver) set(host string, ip [16]byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ips[host] = ip
	s.ttls[host] = ttl
}

func (s *stubResolver) setFail(host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail[host] = true
}

func (s *stubResolver) clearFail(host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.fail, host)
}

func (s *stubResolver) Resolve(ctx context.Context, host string) ([16]byte, time.Duration, error) {
	atomic.AddInt64(&s.calls, 1)
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail[host] {
		return [16]byte{}, 0, errors.New("nxdomain")
	}
	ip, ok := s.ips[host]
	if !ok {
		return [16]byte{}, 0, errors.New("no address")
	}
	return ip, s.ttls[host], nil
}

func (s *stubResolver) callCount() int64 { return atomic.LoadInt64(&s.calls) }

// clock is a manual clock a test can advance to expire TTLs deterministically.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func ipv4(a, b, cc, d byte) [16]byte {
	return ipToArray(net.IPv4(a, b, cc, d))
}

func TestLookupMissThenPrefetchThenHit(t *testing.T) {
	stub := newStub()
	want := ipv4(93, 184, 216, 34)
	stub.set("example.com", want, 10*time.Minute)

	clk := newClock()
	c := NewCache(stub, clk.now)
	defer c.Close()

	if _, ok := c.Lookup("example.com"); ok {
		t.Fatal("expected a miss before any prefetch")
	}

	c.Prefetch("example.com")
	c.Wait()

	got, ok := c.Lookup("example.com")
	if !ok {
		t.Fatal("expected a hit after prefetch")
	}
	if got != want {
		t.Fatalf("ip = %v, want %v", got, want)
	}
	if n := stub.callCount(); n != 1 {
		t.Fatalf("resolver calls = %d, want 1", n)
	}
}

func TestTTLClampFloorAndCap(t *testing.T) {
	stub := newStub()
	stub.set("short.test", ipv4(10, 0, 0, 1), 1*time.Second)
	stub.set("long.test", ipv4(10, 0, 0, 2), 100*time.Hour)

	clk := newClock()
	c := NewCache(stub, clk.now,
		WithMinTTL(60*time.Second),
		WithMaxTTL(24*time.Hour),
	)
	defer c.Close()

	if _, ok := c.Resolve(context.Background(), "short.test"); !ok {
		t.Fatal("short.test should resolve")
	}
	if _, ok := c.Resolve(context.Background(), "long.test"); !ok {
		t.Fatal("long.test should resolve")
	}

	// The 1s TTL is floored to MinTTL (60s): still a hit at 59s, gone at 61s.
	clk.advance(59 * time.Second)
	if _, ok := c.Lookup("short.test"); !ok {
		t.Fatal("short.test should still be fresh at 59s (floored to 60s)")
	}
	clk.advance(2 * time.Second)
	if _, ok := c.Lookup("short.test"); ok {
		t.Fatal("short.test should expire just past 60s")
	}

	// The 100h TTL is capped to MaxTTL (24h): still a hit just under, gone past.
	// We are now 61s into the timeline; long.test expires at 24h from t0.
	clk.advance(24*time.Hour - 61*time.Second - time.Second)
	if _, ok := c.Lookup("long.test"); !ok {
		t.Fatal("long.test should still be fresh just under 24h")
	}
	clk.advance(2 * time.Second)
	if _, ok := c.Lookup("long.test"); ok {
		t.Fatal("long.test should expire just past 24h (capped)")
	}
}

func TestNegativeCacheSuppressAndRetry(t *testing.T) {
	stub := newStub()
	stub.setFail("dead.test")

	clk := newClock()
	c := NewCache(stub, clk.now, WithNegativeTTL(5*time.Minute))
	defer c.Close()

	if _, ok := c.Resolve(context.Background(), "dead.test"); ok {
		t.Fatal("dead.test should fail to resolve")
	}
	if !c.Suppressed("dead.test") {
		t.Fatal("dead.test should be suppressed after a failure")
	}
	if got := c.Failures("dead.test"); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}

	// A Prefetch while suppressed must not call the resolver again.
	before := stub.callCount()
	c.Prefetch("dead.test")
	c.Wait()
	if stub.callCount() != before {
		t.Fatal("a suppressed host must not be re-resolved")
	}

	// Still suppressed just before the negative TTL lapses.
	clk.advance(5*time.Minute - time.Second)
	if !c.Suppressed("dead.test") {
		t.Fatal("dead.test should still be suppressed before negative TTL ends")
	}

	// Past the negative TTL the host is no longer suppressed and a retry runs.
	// Make it resolve this time and confirm the negative entry clears.
	clk.advance(2 * time.Second)
	if c.Suppressed("dead.test") {
		t.Fatal("dead.test should no longer be suppressed after negative TTL")
	}
	stub.clearFail("dead.test")
	stub.set("dead.test", ipv4(8, 8, 8, 8), 10*time.Minute)
	if _, ok := c.Resolve(context.Background(), "dead.test"); !ok {
		t.Fatal("dead.test should resolve after the negative TTL")
	}
	if c.Suppressed("dead.test") {
		t.Fatal("a successful resolve should clear the negative entry")
	}
}

func TestRepeatedFailuresCountUp(t *testing.T) {
	stub := newStub()
	stub.setFail("dead.test")

	clk := newClock()
	c := NewCache(stub, clk.now, WithNegativeTTL(1*time.Minute))
	defer c.Close()

	for i := 1; i <= 3; i++ {
		c.Resolve(context.Background(), "dead.test")
		// Step past the negative window so the next Resolve actually retries.
		clk.advance(2 * time.Minute)
		if got := c.Failures("dead.test"); got != i {
			t.Fatalf("after %d attempts failures = %d, want %d", i, got, i)
		}
	}
}

func TestInflightDedup(t *testing.T) {
	stub := newStub()
	stub.set("busy.test", ipv4(1, 2, 3, 4), 10*time.Minute)
	stub.block = make(chan struct{})

	clk := newClock()
	c := NewCache(stub, clk.now, WithWorkers(8))
	defer c.Close()

	// Queue the same host many times while the first resolve is parked. Only one
	// should ever reach the resolver.
	for i := 0; i < 200; i++ {
		c.Prefetch("busy.test")
	}

	// Give the workers a moment to pull from the queue, then release the block.
	time.Sleep(20 * time.Millisecond)
	close(stub.block)
	c.Wait()

	if n := stub.callCount(); n != 1 {
		t.Fatalf("resolver calls = %d, want 1 (in-flight dedup)", n)
	}
	if _, ok := c.Lookup("busy.test"); !ok {
		t.Fatal("busy.test should be cached after the resolve completes")
	}
}

func TestDialAddrRewriteAndPassthrough(t *testing.T) {
	stub := newStub()
	cachedIP := ipv4(203, 0, 113, 5)
	stub.set("cached.test", cachedIP, 10*time.Minute)

	clk := newClock()
	c := NewCache(stub, clk.now)
	defer c.Close()

	if _, ok := c.Resolve(context.Background(), "cached.test"); !ok {
		t.Fatal("cached.test should resolve")
	}

	// Cached host: addr is rewritten to the cached IP, port preserved.
	if got, want := c.dialAddr("cached.test:443"), net.JoinHostPort(arrayToIP(cachedIP).String(), "443"); got != want {
		t.Fatalf("cached dial addr = %q, want %q", got, want)
	}
	// Uncached host: addr passes through unchanged.
	if got := c.dialAddr("uncached.test:80"); got != "uncached.test:80" {
		t.Fatalf("uncached dial addr = %q, want passthrough", got)
	}
	// A bare address with no port passes through unchanged.
	if got := c.dialAddr("noport"); got != "noport" {
		t.Fatalf("malformed addr = %q, want passthrough", got)
	}
}

// TestDialContextDialsCachedIP runs the returned DialContext against a real
// loopback listener to confirm it actually dials the cached IP. The cache holds
// 127.0.0.1 for the host, so dialing "host:port" reaches the listener even
// though the host name has no DNS record.
func TestDialContextDialsCachedIP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	stub := newStub()
	stub.set("loopback.test", ipToArray(net.ParseIP("127.0.0.1")), 10*time.Minute)

	c := NewCache(stub, nil)
	defer c.Close()
	if _, ok := c.Resolve(context.Background(), "loopback.test"); !ok {
		t.Fatal("loopback.test should resolve")
	}

	dial := c.DialContext(&net.Dialer{})
	conn, err := dial(context.Background(), "tcp", net.JoinHostPort("loopback.test", port))
	if err != nil {
		t.Fatalf("dial cached host: %v", err)
	}
	conn.Close()
}

func TestConcurrentPrefetchAndLookup(t *testing.T) {
	stub := newStub()
	hosts := []string{"a.test", "b.test", "c.test", "d.test", "e.test"}
	for i, h := range hosts {
		stub.set(h, ipv4(10, 0, 0, byte(i+1)), 10*time.Minute)
	}

	clk := newClock()
	c := NewCache(stub, clk.now, WithWorkers(4))
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				h := hosts[i%len(hosts)]
				c.Prefetch(h)
				c.Lookup(h)
				c.Suppressed(h)
				c.Stats()
			}
		}()
	}
	wg.Wait()
	c.Wait()

	for _, h := range hosts {
		if _, ok := c.Lookup(h); !ok {
			t.Fatalf("%s should be cached after the storm", h)
		}
	}
}

func TestPrefetchSkipsFreshHost(t *testing.T) {
	stub := newStub()
	stub.set("fresh.test", ipv4(10, 1, 1, 1), 10*time.Minute)

	clk := newClock()
	c := NewCache(stub, clk.now)
	defer c.Close()

	c.Prefetch("fresh.test")
	c.Wait()
	first := stub.callCount()

	// Already fresh, so a second prefetch should not hit the resolver.
	c.Prefetch("fresh.test")
	c.Wait()
	if stub.callCount() != first {
		t.Fatal("a fresh host should not be re-resolved")
	}
}

func TestStats(t *testing.T) {
	stub := newStub()
	stub.set("ok.test", ipv4(10, 2, 2, 2), 10*time.Minute)
	stub.setFail("bad.test")

	clk := newClock()
	c := NewCache(stub, clk.now)
	defer c.Close()

	c.Resolve(context.Background(), "ok.test")
	c.Resolve(context.Background(), "bad.test")

	s := c.Stats()
	if s.Positive != 1 {
		t.Fatalf("positive = %d, want 1", s.Positive)
	}
	if s.Negative != 1 {
		t.Fatalf("negative = %d, want 1", s.Negative)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c := NewCache(newStub(), nil)
	c.Close()
	c.Close()
}

func TestIPRoundTrip(t *testing.T) {
	cases := []net.IP{
		net.IPv4(1, 2, 3, 4),
		net.ParseIP("2001:db8::1"),
	}
	for _, ip := range cases {
		a := ipToArray(ip)
		back := arrayToIP(a)
		if !back.Equal(ip) {
			t.Fatalf("round trip %v -> %v", ip, back)
		}
	}
}
