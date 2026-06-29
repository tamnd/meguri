package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/prioritize"
)

// TestImportHostSignalResident checks an imported host_score lands straight on a
// host already resident in the frontier, the common case where the host has
// already been seeded by the time its tsumugi signal arrives.
func TestImportHostSignalResident(t *testing.T) {
	f := New(1, 0)
	f.Seed("http://h.example/x", "h.example", 0.5, 0, 0, 10)
	hk := meguri.HostKeyOf("h.example")
	f.ImportHostSignal(meguri.HostSignal{HostKey: hk, HostScore: 0.9})
	if got := f.hosts[hk].rec.HostScore; got != 0.9 {
		t.Fatalf("resident host score = %v, want 0.9", got)
	}
}

// TestImportHostSignalParkedThenStamped checks a host_score that arrives before
// the host is seen is parked and then stamped onto the record when newHost first
// builds it, so an import that races ahead of the host is not lost.
func TestImportHostSignalParkedThenStamped(t *testing.T) {
	f := New(1, 0)
	hk := meguri.HostKeyOf("late.example")
	f.ImportHostSignal(meguri.HostSignal{HostKey: hk, HostScore: 0.7})
	if _, parked := f.importedHostScore[hk]; !parked {
		t.Fatal("score for an unseen host was not parked")
	}
	f.Seed("http://late.example/y", "late.example", 0.5, 0, 0, 10)
	if got := f.hosts[hk].rec.HostScore; got != 0.7 {
		t.Fatalf("stamped host score = %v, want 0.7", got)
	}
	if _, still := f.importedHostScore[hk]; still {
		t.Fatal("parked entry was not cleared once the host was seeded")
	}
}

// TestImportURLSignalRaisesPriority checks an imported per-page PageRank flows
// into the blend: a URL scored once with no import and again after a high rank
// imports comes out strictly higher, so the signal actually moves priority rather
// than only being recorded.
func TestImportURLSignalRaisesPriority(t *testing.T) {
	f := New(1, 0, WithPrioritizer(prioritize.DefaultParams()))
	uk := meguri.MakeURLKey("p.example", "/page")
	rec := &meguri.URLRecord{URLKey: uk}
	h := &meguri.HostRecord{HostKey: uk.HostKey}

	before := f.prio.Priority(rec, h)
	f.ImportURLSignal(meguri.URLSignal{URLKey: uk, PageRank: 1.0})
	after := f.prio.Priority(rec, h)
	if !(after > before) {
		t.Fatalf("imported PageRank did not raise priority: before %v, after %v", before, after)
	}
}
