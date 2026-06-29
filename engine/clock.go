package engine

import (
	"context"
	"sync/atomic"
	"time"
)

// Clock is the engine's view of time. The run loop reads Now in epoch-seconds to
// stamp dispatch and outcome folds, and calls SleepUntil when every host is
// cooling down and nothing is in flight, so the next thing it does is wait for
// the earliest politeness window to open. Two implementations bind it: a wall
// clock that actually sleeps for a live crawl, and a logical clock that jumps the
// instant forward so a corpus replay drains without real waits.
//
// SleepUntil is called only when the pool is empty (no fetch is in flight), so a
// logical clock can advance the instant with no risk of reordering an in-flight
// fetch against the clock.
type Clock interface {
	Now() uint32
	SleepUntil(ctx context.Context, epochSec uint32)
}

// WallClock is the live-crawl clock: Now is the real Unix second and SleepUntil
// blocks until that second arrives or the context is cancelled, whichever comes
// first, so a cancelled run never sits in a politeness wait.
type WallClock struct{}

// Now returns the current Unix time in seconds.
func (WallClock) Now() uint32 { return uint32(time.Now().Unix()) }

// SleepUntil blocks until epochSec or context cancellation.
func (WallClock) SleepUntil(ctx context.Context, epochSec uint32) {
	d := time.Until(time.Unix(int64(epochSec), 0))
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// LogicalClock is the replay clock the corpus gate runs: SleepUntil jumps the
// instant forward instead of waiting, so a slice whose politeness windows span
// hours drains in milliseconds while the dispatch order stays exactly what the
// wall clock would produce. The instant only ever moves forward, and only when
// the pool is empty, so a concurrent fold never sees the clock move under it.
type LogicalClock struct{ now atomic.Uint32 }

// NewLogicalClock starts a logical clock at start (epoch-seconds).
func NewLogicalClock(start uint32) *LogicalClock {
	c := &LogicalClock{}
	c.now.Store(start)
	return c
}

// Now returns the current logical instant.
func (c *LogicalClock) Now() uint32 { return c.now.Load() }

// SleepUntil advances the logical instant to epochSec if it is in the future,
// the jump that stands in for a wall-clock wait.
func (c *LogicalClock) SleepUntil(_ context.Context, epochSec uint32) {
	for {
		cur := c.now.Load()
		if epochSec <= cur {
			return
		}
		if c.now.CompareAndSwap(cur, epochSec) {
			return
		}
	}
}
