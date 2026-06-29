package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
)

// TestStatsCountsSeededFrontier gates Frontier.Stats on a deterministic seeded
// frontier: the totals, the per-status histogram, the seen-set occupancy, and the
// time-dependent due count all fold from a known input, so the stats command reads
// numbers a reader can verify by hand. Fifteen URLs across three hosts are due now;
// two more on a fourth host are dated into the future and only count as due once
// the clock reaches their hour.
func TestStatsCountsSeededFrontier(t *testing.T) {
	f := New(0, 0)

	hosts := []string{"a.example", "b.example", "c.example"}
	for _, h := range hosts {
		for i := range 5 {
			f.Seed("https://"+h+"/p/"+string(rune('a'+i)), h, 0.5, 0, 0, 10)
		}
	}
	// Two URLs dated far into the future on a fourth host: present, scheduled, but
	// not due until the clock reaches hour 5000.
	for i := range 2 {
		f.Seed("https://d.example/late/"+string(rune('a'+i)), "d.example", 0.5, 0, 5000, 10)
	}

	st := f.Stats(0)
	if st.URLs != 17 {
		t.Fatalf("URLs = %d, want 17", st.URLs)
	}
	if st.Hosts != 4 {
		t.Fatalf("Hosts = %d, want 4", st.Hosts)
	}
	if st.Pending != 17 {
		t.Fatalf("Pending = %d, want 17", st.Pending)
	}
	if got := st.ByStatus[meguri.StatusScheduled]; got != 17 {
		t.Fatalf("ByStatus[scheduled] = %d, want 17", got)
	}
	if st.Due != 15 {
		t.Fatalf("Due at hour 0 = %d, want 15 (the future-dated two are not due yet)", st.Due)
	}
	if st.SeenKeys != 17 {
		t.Fatalf("SeenKeys = %d, want 17 (every seed inserts into the seen-set)", st.SeenKeys)
	}
	if st.SeenBitsPerURL <= 0 {
		t.Fatalf("SeenBitsPerURL = %.2f, want > 0", st.SeenBitsPerURL)
	}

	// Advancing the clock past the future hour brings the deferred two into the due
	// set, the time-dependence the stats command reports.
	if later := f.Stats(5000); later.Due != 17 {
		t.Fatalf("Due at hour 5000 = %d, want 17", later.Due)
	}
}
