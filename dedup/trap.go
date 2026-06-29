package dedup

import "github.com/tamnd/meguri"

// Admit is the blunt always-correct trap defense of doc 08, section 8.2 (D17):
// per-host url_budget and depth_cap decide whether a new discovery enters the
// frontier or is parked in Trapped. It does not need to understand why a host is
// exploding, only that it is. A zero budget or zero depth cap means unlimited
// (doc 09 sets the real numbers); a flagged host's discoveries must additionally
// pass the pattern heuristics.
//
// Trapped is a parked state, not a dropped URL: the caller keeps the row, so the
// URL is in the seen-set and dedups rediscoveries, and it can be released back to
// Scheduled if the host's budget is raised or the flag is cleared.
func Admit(depth uint16, h *meguri.HostRecord, passesHeuristics bool) meguri.URLStatus {
	if h.DepthCap > 0 && depth > h.DepthCap {
		return meguri.StatusTrapped // too deep under this host
	}
	if h.URLBudget > 0 && h.URLCount >= h.URLBudget {
		return meguri.StatusTrapped // host budget exhausted
	}
	if h.Flags&meguri.HostFlagTrapSuspect != 0 && !passesHeuristics {
		return meguri.StatusTrapped // flagged host, failed the pattern check
	}
	return meguri.StatusScheduled
}

// softThreshold is how many distinct URLKeys on one host must return the same
// content fingerprint before the boilerplate is recognized as a soft-404
// template (doc 08, section 8.6). A real host serves distinct content per URL, so
// a fingerprint repeated across many keys is the host's not-found response.
const softThreshold = 8

// SoftDetector recognizes soft-404 farms by content fingerprint: a server that
// returns 200 OK with the same "not found" body for any URL defeats the 404 stop
// signal, but all those URLs share one content_fp, so counting distinct keys per
// fingerprint per host surfaces the template (doc 08, section 8.6). It also feeds
// HostFlagTrapSuspect: a host serving one boilerplate for many URLs is a trap
// suspect.
type SoftDetector struct {
	threshold int
	// per host, per fingerprint, the set of distinct keys seen with it.
	seen map[uint64]map[uint64]map[meguri.URLKey]struct{}
	// fingerprints already confirmed as a soft-404 template, per host.
	template map[uint64]map[uint64]struct{}
}

// NewSoftDetector returns a soft-404 detector at the default threshold.
func NewSoftDetector() *SoftDetector {
	return &SoftDetector{
		threshold: softThreshold,
		seen:      make(map[uint64]map[uint64]map[meguri.URLKey]struct{}),
		template:  make(map[uint64]map[uint64]struct{}),
	}
}

// WithThreshold sets how many distinct keys sharing a fingerprint mark a soft-404
// template.
func (d *SoftDetector) WithThreshold(n int) *SoftDetector {
	if n > 0 {
		d.threshold = n
	}
	return d
}

// Observe records that key on host returned content fingerprint fp, and reports
// whether fp is now a recognized soft-404 template. Once a fingerprint crosses
// the threshold of distinct keys, it is a template, and every subsequent URL on
// the host returning it should be treated as Gone rather than Crawled, restoring
// the stop signal the soft-404 tried to defeat.
func (d *SoftDetector) Observe(hostKey, fp uint64, key meguri.URLKey) bool {
	if fp == 0 {
		return false
	}
	if d.isTemplate(hostKey, fp) {
		return true
	}
	byFP := d.seen[hostKey]
	if byFP == nil {
		byFP = make(map[uint64]map[meguri.URLKey]struct{})
		d.seen[hostKey] = byFP
	}
	keys := byFP[fp]
	if keys == nil {
		keys = make(map[meguri.URLKey]struct{})
		byFP[fp] = keys
	}
	keys[key] = struct{}{}
	if len(keys) >= d.threshold {
		d.markTemplate(hostKey, fp)
		delete(byFP, fp) // the counting set is no longer needed
		return true
	}
	return false
}

// IsTemplate reports whether fp is a confirmed soft-404 template on the host,
// without recording an observation.
func (d *SoftDetector) IsTemplate(hostKey, fp uint64) bool {
	return d.isTemplate(hostKey, fp)
}

func (d *SoftDetector) isTemplate(hostKey, fp uint64) bool {
	t := d.template[hostKey]
	if t == nil {
		return false
	}
	_, ok := t[fp]
	return ok
}

func (d *SoftDetector) markTemplate(hostKey, fp uint64) {
	t := d.template[hostKey]
	if t == nil {
		t = make(map[uint64]struct{})
		d.template[hostKey] = t
	}
	t[fp] = struct{}{}
}

// FlagTrapSuspect sets the trap-suspect bit on a host, the precise defense that
// tightens admission (doc 08, section 8.4). It is sticky and serialized with the
// HostRecord, so it survives a checkpoint and reload.
func FlagTrapSuspect(h *meguri.HostRecord) {
	h.Flags |= meguri.HostFlagTrapSuspect
}

// IsTrapSuspect reports whether the host is flagged.
func IsTrapSuspect(h *meguri.HostRecord) bool {
	return h.Flags&meguri.HostFlagTrapSuspect != 0
}
