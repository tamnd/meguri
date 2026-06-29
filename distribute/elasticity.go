package distribute

// Elasticity is the operator-facing control loop that turns a backlog signal into
// scale decisions (doc 12, section 7). The mechanisms it drives already exist: a
// partition is added at the high end or removed from it with an epoch bump
// (AddPartition/RemovePartition), the override table pins a draining host, and the
// rebalance ships the moved host slices once the map changes. What was missing was
// the loop that watches the backlog, decides, and drives those mechanisms with
// enough hysteresis that it does not flap. Elasticity is that loop.
//
// It is deliberately clock-free: a Tick is one control interval the caller paces,
// so the loop is deterministic and testable, and the cooldown and breach windows
// are counted in ticks rather than wall time. The control plane stays off the data
// path; the loop only changes the map, and the partitions react to the new epoch
// on their next heartbeat, slicing and shipping the moved hosts themselves.
type Elasticity struct {
	cfg ElasticityConfig
	ctl *Control
	src BacklogSource

	highStreak int // consecutive ticks the load has sat above HighWater
	lowStreak  int // consecutive ticks the load has sat below LowWater
	cooldown   int // ticks left before another action is allowed
}

// BacklogSource reports the pending-work depth of every partition, the signal the
// loop scales on. A fleet binds the partitions' live pending counts; a test binds
// a fixed slice. The slice is indexed by PartitionID, so its length is the current
// partition count the loop sees.
type BacklogSource interface {
	Backlog() []int
}

// ElasticityConfig sets the loop's thresholds and hysteresis. The water marks are
// per-partition backlog: the loop scales on the mean depth across partitions, so a
// fleet that is evenly under water adds capacity and one that is evenly drained
// gives it back. The band between LowWater and HighWater is the hold zone that
// keeps a steady fleet steady.
type ElasticityConfig struct {
	HighWater     int // mean per-partition backlog above which the fleet is under-provisioned
	LowWater      int // mean per-partition backlog below which the fleet is over-provisioned
	MinPartitions int // never remove below this many
	MaxPartitions int // never add above this many
	Breaches      int // consecutive ticks a water mark must hold before the loop acts
	Cooldown      int // ticks to wait after an action before acting again
}

// Scale is one tick's decision.
type Scale uint8

const (
	ScaleHold Scale = iota // load is in band, or hysteresis is not yet satisfied
	ScaleUp                // a partition was added
	ScaleDown              // a partition was removed
)

// NewElasticity builds the loop over a control plane and a backlog source. A
// zero-valued config is filled with working defaults so a caller need only set the
// water marks it cares about.
func NewElasticity(ctl *Control, src BacklogSource, cfg ElasticityConfig) *Elasticity {
	if cfg.MinPartitions < 1 {
		cfg.MinPartitions = 1
	}
	if cfg.MaxPartitions < cfg.MinPartitions {
		cfg.MaxPartitions = cfg.MinPartitions
	}
	if cfg.Breaches < 1 {
		cfg.Breaches = 1
	}
	if cfg.HighWater < 1 {
		cfg.HighWater = 1
	}
	// LowWater may be 0, meaning never scale down on backlog alone.
	return &Elasticity{cfg: cfg, ctl: ctl, src: src}
}

// Tick reads the backlog once and acts at most once: it adds a partition when the
// mean per-partition backlog has sat above HighWater for Breaches consecutive
// ticks and the fleet is below MaxPartitions, removes one when the mean has sat
// below LowWater for Breaches ticks and the fleet is above MinPartitions, and
// otherwise holds. After any action it enters a cooldown of Cooldown ticks during
// which it only holds, so a single load swing never triggers a cascade of
// resizes. It returns the decision so an operator or a test can watch the loop.
func (e *Elasticity) Tick() Scale {
	if e.cooldown > 0 {
		e.cooldown--
		// Hold through cooldown, and reset the streaks so the loop re-confirms a
		// sustained breach after the fleet has had time to settle rather than acting
		// on stale momentum.
		e.highStreak, e.lowStreak = 0, 0
		return ScaleHold
	}

	load := e.meanBacklog()
	n := e.ctl.NumPartitions()

	switch {
	case load > e.cfg.HighWater:
		e.highStreak++
		e.lowStreak = 0
	case e.cfg.LowWater > 0 && load < e.cfg.LowWater:
		e.lowStreak++
		e.highStreak = 0
	default:
		e.highStreak, e.lowStreak = 0, 0
	}

	if e.highStreak >= e.cfg.Breaches && n < e.cfg.MaxPartitions {
		e.ctl.AddPartition("")
		e.afterAction()
		return ScaleUp
	}
	if e.lowStreak >= e.cfg.Breaches && n > e.cfg.MinPartitions {
		if _, ok := e.ctl.RemovePartition(); ok {
			e.afterAction()
			return ScaleDown
		}
	}
	return ScaleHold
}

// afterAction clears the streaks and starts the cooldown after a resize.
func (e *Elasticity) afterAction() {
	e.highStreak, e.lowStreak = 0, 0
	e.cooldown = e.cfg.Cooldown
}

// meanBacklog is the mean pending depth across the partitions the source reports.
// An empty source reads as zero load, which never triggers a scale-up, the safe
// default for a fleet with no signal yet.
func (e *Elasticity) meanBacklog() int {
	b := e.src.Backlog()
	if len(b) == 0 {
		return 0
	}
	total := 0
	for _, v := range b {
		total += v
	}
	return total / len(b)
}
