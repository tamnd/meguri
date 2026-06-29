// Package politeness is meguri's rate brain: the policy that decides how fast a
// single host may be crawled and keeps two independent throttles in step, one
// per host group and one per resolved IP. It owns no network and no state beyond
// the shared per-IP table; the frontier reads its functions to compute a host's
// next-eligible instant and to fold one fetch outcome into the host's adaptive
// interval (doc 07). The split is deliberate: meguri owns the politeness policy,
// ami owns the fetch mechanism.
//
// The model is two one-token buckets reduced to timestamps. A bucket has no
// burst: a fetch may start only at or after its next-eligible instant, and the
// instant advances by one interval measured start-to-start. A host is dispatched
// only when BOTH its host bucket and its IP bucket permit, and dispatching spends
// both. The per-IP bucket is what stops meguri from hammering one machine through
// a hundred vhosts that share an address.
package politeness

import "time"

// Config is the politeness policy: the interval band a host's rate must stay in
// and the AIMD reaction constants that turn fetch outcomes into a crawl rate.
// The defaults come from doc 07: a one-second baseline, a 250ms floor that is
// the physical wall meguri refuses to cross (D19), and a ceiling past which a
// host is treated as dead.
type Config struct {
	Default time.Duration // baseline interval for a host with no signal yet
	Floor   time.Duration // hard minimum; never fetch one host faster than this
	Ceiling time.Duration // hard maximum; a host pinned here is flagged dead
	IPFloor time.Duration // per-IP minimum when many hosts share one address

	Backoff     float64       // multiplicative widen on 429/5xx, >= 1
	SoftWiden   float64       // multiplicative widen on sharply rising latency, >= 1
	Narrow      time.Duration // additive narrow toward the floor on a healthy fetch
	LatencyRise float64       // latency ratio that counts as "rising", e.g. 1.5
}

// DefaultConfig returns the doc 07 baseline policy.
func DefaultConfig() Config {
	return Config{
		Default:     1 * time.Second,
		Floor:       250 * time.Millisecond,
		Ceiling:     5 * time.Minute,
		IPFloor:     500 * time.Millisecond,
		Backoff:     2.0,
		SoftWiden:   1.3,
		Narrow:      100 * time.Millisecond,
		LatencyRise: 1.5,
	}
}

// Clamp bounds d into [lo, hi]. lo wins if the band is inverted.
func Clamp(d, lo, hi time.Duration) time.Duration {
	if hi < lo {
		hi = lo
	}
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}

// HostInterval clamps a host's adaptive interval into the legal band. The lower
// bound is the larger of the global floor and the host's published crawl-delay
// (from robots or config), so a site asking to be crawled slower is always
// honored and meguri never dips under its own physical floor. The upper bound is
// the ceiling.
func (c Config) HostInterval(adaptive, crawlDelay time.Duration) time.Duration {
	lo := max(c.Floor, crawlDelay)
	return Clamp(adaptive, lo, c.Ceiling)
}

// Signal is the part of a fetch Outcome the rate controller reads.
type Signal struct {
	Status      int           // HTTP status, 0 for a transport error
	RetryAfter  time.Duration // Retry-After header value, 0 if none
	Latency     time.Duration // this fetch's round-trip time
	PrevLatency time.Duration // the host's smoothed latency before this fetch
}

// backoff reports whether a status warrants multiplicative backoff: a 429 rate
// limit or any 5xx server error.
func backoff(status int) bool {
	return status == 429 || (status >= 500 && status <= 599)
}

// Adapt is the AIMD step: it maps the current adaptive interval and one fetch
// signal to the next interval, before clamping. The reactions, in order:
//
//   - a 429 or 5xx multiplies the interval by Backoff and honors Retry-After as
//     a floor, the multiplicative increase that backs a struggling host off fast.
//   - sharply rising latency (Latency over PrevLatency*LatencyRise) softly widens
//     by SoftWiden, easing off before the host starts erroring.
//   - an otherwise healthy fetch narrows the interval by Narrow, the additive
//     decrease that creeps a recovered host back toward the floor slowly.
//
// The result is unclamped; the caller applies HostInterval to bound it into the
// host's legal band.
func (c Config) Adapt(adaptive time.Duration, s Signal) time.Duration {
	switch {
	case backoff(s.Status):
		next := time.Duration(float64(adaptive) * c.Backoff)
		return max(next, s.RetryAfter)
	case s.PrevLatency > 0 && s.Latency > time.Duration(float64(s.PrevLatency)*c.LatencyRise):
		return time.Duration(float64(adaptive) * c.SoftWiden)
	default:
		return max(adaptive-c.Narrow, 0)
	}
}

// AtCeiling reports whether the clamped interval has been pinned to the ceiling,
// the condition that, sustained over a fetch run, marks a host dead.
func (c Config) AtCeiling(clamped time.Duration) bool {
	return clamped >= c.Ceiling
}
