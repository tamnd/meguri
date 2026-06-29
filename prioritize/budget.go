package prioritize

import "github.com/tamnd/meguri"

// UpdateHostBudget recomputes a host's url_budget from its cross-host in-degree,
// the count of distinct other host groups that link to it (STAR, doc 09: Lee,
// Leonard, Wang, Loguinov, "IRLbot: Scaling to 6 Billion Pages and Beyond," WWW
// 2008). This is the reputation signal an adversary cannot forge: a spammer can
// mint hostnames without bound but cannot mint independent domains that vouch for
// it, so cross-host in-degree is budget that has to be earned. A floor lets a
// brand-new host be discovered at all; a cap stops any one host claiming the
// whole partition's budget.
func UpdateHostBudget(h *meguri.HostRecord, crossHostInDegree uint32, p Params) {
	budget := min(max(p.BaseBudget+p.PerInLink*crossHostInDegree, p.MinBudget), p.MaxBudget)
	h.URLBudget = budget
}

// Admit decides what state a newly discovered URL enters, enforcing the host's
// budget by deferral, not discard (BEAST, doc 09). An over-budget or too-deep URL
// is parked in Trapped: the row stays in the frontier and the seen-set so it
// dedups rediscoveries, and it can be released back to Scheduled if the host's
// budget later rises. A zero budget or zero depth cap means unlimited (the blunt
// M2 default before M5 sets the real numbers). This mirrors dedup.Admit and is
// the prioritization-owned statement of the same rule.
func Admit(h *meguri.HostRecord, depth uint16) meguri.URLStatus {
	if h.DepthCap > 0 && depth > h.DepthCap {
		return meguri.StatusTrapped
	}
	if h.URLBudget > 0 && h.URLCount >= h.URLBudget {
		return meguri.StatusTrapped
	}
	return meguri.StatusScheduled
}

// CrossHostInDegree tracks, per target host, the set of distinct source hosts
// that have linked into it (doc 09, section "Budget by cross-host in-degree").
// A link from a page on the same host group does not count; only a distinct
// other host group adds reputation, which is the spam defense. The exact set is
// resident here for the single-partition gate; at fleet scale this is an
// approximate distinct counter, the same approximate accumulation D16 sanctions
// for the cross-partition signal, so a target's in-degree memory stays bounded.
type CrossHostInDegree struct {
	srcByTarget map[uint64]map[uint64]struct{}
}

// NewCrossHostInDegree returns an empty tracker.
func NewCrossHostInDegree() *CrossHostInDegree {
	return &CrossHostInDegree{srcByTarget: make(map[uint64]map[uint64]struct{})}
}

// Observe records that a link from srcHost reached targetHost and reports the
// target's new distinct cross-host in-degree. A same-host link (srcHost ==
// targetHost) is ignored and returns the count unchanged: it carries no
// reputation, which is exactly what stops a spam farm inflating its budget with
// dense internal links.
func (c *CrossHostInDegree) Observe(targetHost, srcHost uint64) uint32 {
	if srcHost == targetHost {
		return c.Count(targetHost)
	}
	set := c.srcByTarget[targetHost]
	if set == nil {
		set = make(map[uint64]struct{})
		c.srcByTarget[targetHost] = set
	}
	set[srcHost] = struct{}{}
	return uint32(len(set))
}

// Count returns the distinct cross-host in-degree recorded for a host.
func (c *CrossHostInDegree) Count(targetHost uint64) uint32 {
	return uint32(len(c.srcByTarget[targetHost]))
}
