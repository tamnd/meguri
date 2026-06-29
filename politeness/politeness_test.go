package politeness

import (
	"testing"
	"time"
)

// TestHostIntervalRespectsCrawlDelay checks the lower bound is the larger of the
// floor and the published crawl-delay, so a slow-crawl request always wins and
// the floor is never crossed.
func TestHostIntervalRespectsCrawlDelay(t *testing.T) {
	c := DefaultConfig()
	tests := []struct {
		name       string
		adaptive   time.Duration
		crawlDelay time.Duration
		want       time.Duration
	}{
		{"floor wins when no crawl-delay", 10 * time.Millisecond, 0, c.Floor},
		{"crawl-delay wins over floor", 100 * time.Millisecond, 2 * time.Second, 2 * time.Second},
		{"adaptive in band passes", 3 * time.Second, 1 * time.Second, 3 * time.Second},
		{"ceiling caps a runaway", 10 * time.Minute, 1 * time.Second, c.Ceiling},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.HostInterval(tt.adaptive, tt.crawlDelay); got != tt.want {
				t.Errorf("HostInterval(%v,%v) = %v, want %v", tt.adaptive, tt.crawlDelay, got, tt.want)
			}
		})
	}
}

// TestAdaptBacksOffOnError checks a 429 and a 5xx both multiply the interval by
// the backoff factor, the multiplicative increase of AIMD.
func TestAdaptBacksOffOnError(t *testing.T) {
	c := DefaultConfig()
	base := 1 * time.Second
	for _, status := range []int{429, 500, 503} {
		got := c.Adapt(base, Signal{Status: status})
		want := time.Duration(float64(base) * c.Backoff)
		if got != want {
			t.Errorf("Adapt status %d = %v, want %v", status, got, want)
		}
	}
}

// TestAdaptHonorsRetryAfter checks a Retry-After longer than the backed-off
// interval becomes the floor, so a server's explicit ask is obeyed.
func TestAdaptHonorsRetryAfter(t *testing.T) {
	c := DefaultConfig()
	got := c.Adapt(1*time.Second, Signal{Status: 503, RetryAfter: 30 * time.Second})
	if got != 30*time.Second {
		t.Errorf("Adapt with Retry-After = %v, want 30s", got)
	}
}

// TestAdaptWidensOnRisingLatency checks a fetch whose latency jumps past the
// rise ratio softly widens the interval, easing off before errors start.
func TestAdaptWidensOnRisingLatency(t *testing.T) {
	c := DefaultConfig()
	got := c.Adapt(1*time.Second, Signal{Status: 200, Latency: 900 * time.Millisecond, PrevLatency: 100 * time.Millisecond})
	want := time.Duration(float64(time.Second) * c.SoftWiden)
	if got != want {
		t.Errorf("Adapt rising latency = %v, want %v", got, want)
	}
}

// TestAdaptNarrowsWhenHealthy checks a clean fetch additively narrows the
// interval toward the floor, the slow recovery of AIMD.
func TestAdaptNarrowsWhenHealthy(t *testing.T) {
	c := DefaultConfig()
	got := c.Adapt(1*time.Second, Signal{Status: 200, Latency: 100 * time.Millisecond, PrevLatency: 100 * time.Millisecond})
	want := time.Second - c.Narrow
	if got != want {
		t.Errorf("Adapt healthy = %v, want %v", got, want)
	}
}

// TestAdaptConvergesToFloor runs the loop the controller actually runs: many
// healthy fetches in a row must walk the interval down to the floor and stop
// there, never below it, when clamped each step.
func TestAdaptConvergesToFloor(t *testing.T) {
	c := DefaultConfig()
	d := 5 * time.Second
	for range 200 {
		d = c.HostInterval(c.Adapt(d, Signal{Status: 200}), 0)
	}
	if d != c.Floor {
		t.Errorf("converged to %v, want floor %v", d, c.Floor)
	}
}

// TestAdaptBackoffThenRecover checks the full AIMD shape on a host that errors
// then heals: a burst of 5xx widens it fast, a long healthy run walks it back to
// the floor.
func TestAdaptBackoffThenRecover(t *testing.T) {
	c := DefaultConfig()
	d := c.HostInterval(c.Default, 0)
	for range 5 {
		d = c.HostInterval(c.Adapt(d, Signal{Status: 503}), 0)
	}
	if d <= c.Default {
		t.Fatalf("after 5 errors interval %v did not exceed default %v", d, c.Default)
	}
	for range 500 {
		d = c.HostInterval(c.Adapt(d, Signal{Status: 200}), 0)
	}
	if d != c.Floor {
		t.Errorf("after recovery interval %v, want floor %v", d, c.Floor)
	}
}

// TestClamp checks the bound helper, including an inverted band.
func TestClamp(t *testing.T) {
	if got := Clamp(5, 1, 10); got != 5 {
		t.Errorf("in-band = %v, want 5", got)
	}
	if got := Clamp(0, 1, 10); got != 1 {
		t.Errorf("below = %v, want 1", got)
	}
	if got := Clamp(20, 1, 10); got != 10 {
		t.Errorf("above = %v, want 10", got)
	}
	if got := Clamp(5, 10, 1); got != 10 {
		t.Errorf("inverted band = %v, want lo 10", got)
	}
}
