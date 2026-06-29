package politeness

import (
	"testing"
	"time"
)

// BenchmarkAdapt measures one AIMD step: the controller folds a fetch outcome
// into a host's interval on every reported fetch, so at 100B pages this runs
// once per fetch.
func BenchmarkAdapt(b *testing.B) {
	c := DefaultConfig()
	d := time.Second
	b.ReportAllocs()
	for b.Loop() {
		d = c.HostInterval(c.Adapt(d, Signal{Status: 200, Latency: 120 * time.Millisecond, PrevLatency: 100 * time.Millisecond}), time.Second)
	}
}

// BenchmarkIPTableSpend measures one per-IP bucket spend plus its eligibility
// read, the shared-address bookkeeping done on every dispatch.
func BenchmarkIPTableSpend(b *testing.B) {
	tab := NewIPTable(500 * time.Millisecond)
	var addr [16]byte
	addr[15] = 1
	now := int64(0)
	b.ReportAllocs()
	for b.Loop() {
		tab.Spend(addr, now, time.Second)
		_ = tab.EligibleAt(addr)
		now++
	}
}
