package dedup

import (
	"testing"

	"github.com/tamnd/meguri"
)

// TestAdmitDepthCap parks a discovery deeper than the host's depth cap, the blunt
// always-correct trap defense (doc 08, section 8.2).
func TestAdmitDepthCap(t *testing.T) {
	h := &meguri.HostRecord{DepthCap: 5, URLBudget: 1000}
	if got := Admit(3, h, true); got != meguri.StatusScheduled {
		t.Errorf("depth 3 under cap 5 = %v, want Scheduled", got)
	}
	if got := Admit(6, h, true); got != meguri.StatusTrapped {
		t.Errorf("depth 6 over cap 5 = %v, want Trapped", got)
	}
}

// TestAdmitURLBudget parks a discovery once the host's URL budget is exhausted.
func TestAdmitURLBudget(t *testing.T) {
	h := &meguri.HostRecord{DepthCap: 100, URLBudget: 10, URLCount: 10}
	if got := Admit(1, h, true); got != meguri.StatusTrapped {
		t.Errorf("at-budget host = %v, want Trapped", got)
	}
	h.URLCount = 9
	if got := Admit(1, h, true); got != meguri.StatusScheduled {
		t.Errorf("under-budget host = %v, want Scheduled", got)
	}
}

// TestAdmitUnlimited checks the zero-means-unlimited convention so a host with no
// configured budget (doc 09 sets the real numbers) admits normally.
func TestAdmitUnlimited(t *testing.T) {
	h := &meguri.HostRecord{} // zero cap and budget
	if got := Admit(9999, h, true); got != meguri.StatusScheduled {
		t.Errorf("unconfigured host = %v, want Scheduled", got)
	}
}

// TestAdmitFlaggedHost checks a trap-suspect host parks discoveries that fail the
// pattern heuristics, the precise defense layered on the blunt one (doc 08, 8.4).
func TestAdmitFlaggedHost(t *testing.T) {
	h := &meguri.HostRecord{DepthCap: 100, URLBudget: 1000, Flags: meguri.HostFlagTrapSuspect}
	if got := Admit(1, h, false); got != meguri.StatusTrapped {
		t.Errorf("flagged host failing heuristics = %v, want Trapped", got)
	}
	if got := Admit(1, h, true); got != meguri.StatusScheduled {
		t.Errorf("flagged host passing heuristics = %v, want Scheduled", got)
	}
}

// TestSoftDetector checks soft-404 recognition: once enough distinct URLs on a
// host return the same content fingerprint, the boilerplate is a template and
// subsequent URLs returning it are recognized (doc 08, section 8.6).
func TestSoftDetector(t *testing.T) {
	d := NewSoftDetector().WithThreshold(4)
	const host = uint64(0x1234)
	const boilerplate = uint64(0xDEAD)

	// The first three distinct keys do not yet cross the threshold.
	for i := range 3 {
		if d.Observe(host, boilerplate, keyN(i)) {
			t.Fatalf("template recognized too early at key %d", i)
		}
	}
	// The fourth distinct key crosses it.
	if !d.Observe(host, boilerplate, keyN(3)) {
		t.Fatal("template not recognized at the threshold")
	}
	// Any later URL returning the template fingerprint is recognized.
	if !d.Observe(host, boilerplate, keyN(99)) {
		t.Fatal("known template not recognized on a later URL")
	}
	if !d.IsTemplate(host, boilerplate) {
		t.Fatal("IsTemplate false for a confirmed template")
	}
	// A different fingerprint on the same host is not a template.
	if d.IsTemplate(host, 0xBEEF) {
		t.Fatal("unrelated fingerprint reported as a template")
	}
}

// TestSoftDetectorPerHost checks the counting is per host: the same fingerprint
// on different hosts does not pool toward one threshold.
func TestSoftDetectorPerHost(t *testing.T) {
	d := NewSoftDetector().WithThreshold(3)
	const fp = uint64(0xAAAA)
	if d.Observe(1, fp, keyN(0)) || d.Observe(2, fp, keyN(1)) || d.Observe(3, fp, keyN(2)) {
		t.Fatal("one observation on each of three hosts wrongly crossed a per-host threshold")
	}
}

// TestFlagHelpers checks the trap-suspect flag round-trips through the host
// record so it survives a checkpoint (doc 08, section 8.4).
func TestFlagHelpers(t *testing.T) {
	h := &meguri.HostRecord{}
	if IsTrapSuspect(h) {
		t.Fatal("fresh host already flagged")
	}
	FlagTrapSuspect(h)
	if !IsTrapSuspect(h) {
		t.Fatal("flag did not set")
	}
	if h.Flags&meguri.HostFlagTrapSuspect == 0 {
		t.Fatal("flag bit not set in the record")
	}
}
