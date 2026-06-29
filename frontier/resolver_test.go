package frontier

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/meguri/dns"
)

// countResolver counts how many times each host is actually resolved, so a test
// can prove the shared cache collapses repeated work across partitions.
type countResolver struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *countResolver) Resolve(_ context.Context, host string) ([16]byte, time.Duration, error) {
	c.mu.Lock()
	c.n[host]++
	c.mu.Unlock()
	return [16]byte{10, 0, 0, 1}, time.Hour, nil
}

func (c *countResolver) count(host string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[host]
}

// TestSharedResolverCacheResolvesOnce checks two partitions on one machine that
// share a resolver cache resolve a common host exactly once: the second
// partition's prefetch hits the warm entry (or the in-flight reservation) the
// first one left, the machine-local sharing of doc 11.
func TestSharedResolverCacheResolvesOnce(t *testing.T) {
	cr := &countResolver{n: map[string]int{}}
	cache := dns.NewCache(cr, nil)
	defer cache.Close()

	fa := New(1, 0, WithResolverCache(cache))
	fb := New(2, 0, WithResolverCache(cache))
	fa.Seed("http://shared.example/a", "shared.example", 0.5, 0, 0, 10)
	fb.Seed("http://shared.example/b", "shared.example", 0.5, 0, 0, 10)
	cache.Wait()

	if got := cr.count("shared.example"); got != 1 {
		t.Fatalf("shared host resolved %d times across two partitions, want 1", got)
	}
}

// TestResolverCacheSharedLookup checks a host resolved through one partition is
// visible to another that shares the same cache, so the second never has to
// resolve it on its own dispatch path.
func TestResolverCacheSharedLookup(t *testing.T) {
	cr := &countResolver{n: map[string]int{}}
	cache := dns.NewCache(cr, nil)
	defer cache.Close()

	fa := New(1, 0, WithResolverCache(cache))
	fa.Seed("http://warm.example/a", "warm.example", 0.5, 0, 0, 10)
	cache.Wait()

	// A second partition that shares the cache sees the warm address without
	// resolving it itself.
	if _, ok := cache.Lookup("warm.example"); !ok {
		t.Fatal("shared cache missing the host the first partition resolved")
	}
	fb := New(2, 0, WithResolverCache(cache))
	if fb.resolver != cache {
		t.Fatal("second partition did not adopt the shared cache")
	}
}

// TestNilResolverCacheIgnored checks a nil shared cache is ignored, leaving the
// frontier resolver-less rather than panicking on a later dispatch.
func TestNilResolverCacheIgnored(t *testing.T) {
	f := New(1, 0, WithResolverCache(nil))
	if f.resolver != nil {
		t.Fatal("nil shared cache should leave the frontier resolver-less")
	}
}
