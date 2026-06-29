package dedup

import (
	"fmt"
	"testing"
	"time"

	"github.com/tamnd/meguri"
)

// hoursAt returns the epoch-hours of a calendar date, the campaign clock the
// detector reads. It uses time.Date (a deterministic constructor, no wall clock)
// so the tests pin a fixed "now" and never depend on when they run.
func hoursAt(y int, m time.Month, d int) uint32 {
	return uint32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 3600)
}

// trapKey derives a URLKey for a canonical URL the way the discovery path does, so
// the per-host accumulation routes by the same HostKey the frontier uses.
func trapKey(t *testing.T, canon string) (uint64, meguri.URLKey) {
	t.Helper()
	key, _, ok := Key(canon, meguri.GroupRegistrableDomain)
	if !ok {
		t.Fatalf("key %q: not a frontier URL", canon)
	}
	return key.HostKey, key
}

// TestCivilFromDays cross-checks the pure date conversion against the standard
// library across epoch, leap days, and century boundaries, since the calendar
// horizon rests on it.
func TestCivilFromDays(t *testing.T) {
	dates := []struct{ y, m, d int }{
		{1970, 1, 1}, {2000, 2, 29}, {2026, 6, 30},
		{2031, 12, 31}, {1999, 12, 31}, {2100, 3, 1},
	}
	for _, want := range dates {
		days := int(time.Date(want.y, time.Month(want.m), want.d, 0, 0, 0, 0, time.UTC).Unix() / 86400)
		y, m, d := civilFromDays(days)
		if y != want.y || m != want.m || d != want.d {
			t.Errorf("civilFromDays(%d) = %04d-%02d-%02d, want %04d-%02d-%02d", days, y, m, d, want.y, want.m, want.d)
		}
	}
}

// TestViolationCalendar checks the calendar rule fires only on a date past the
// horizon: a far-future month is a trap, a past archive and a within-horizon
// upcoming date are not, and a URL with no date is never a calendar trap.
func TestViolationCalendar(t *testing.T) {
	d := NewTrapDetector()
	now := hoursAt(2026, 6, 1)
	cases := []struct {
		url  string
		want PatternKind
	}{
		{"https://events.example.org/2031/07/", PatternCalendar}, // five years out
		{"https://events.example.org/2028/01/", PatternCalendar}, // past the 12-month horizon
		{"https://events.example.org/2026/07/", PatternNone},     // within the horizon
		{"https://blog.example.org/2019/04/a-post", PatternNone}, // a real past archive
		{"https://shop.example.org/p/4821/detail", PatternNone},  // a four-digit id, not a year
		{"https://cal.example.org/?year=2033&month=9", PatternCalendar},
		{"https://cal.example.org/?date=2030-11-02", PatternCalendar},
		{"https://cal.example.org/?date=2024-11-02", PatternNone},
	}
	for _, c := range cases {
		if got := d.Violation(c.url, now); got != c.want {
			t.Errorf("Violation(%q) = %v, want %v", c.url, got, c.want)
		}
	}
}

// TestViolationFaceted checks the facet rule fires on stacked filter parameters and
// leaves a lightly-filtered listing alone.
func TestViolationFaceted(t *testing.T) {
	d := NewTrapDetector()
	now := hoursAt(2026, 6, 1)
	cases := []struct {
		url  string
		want PatternKind
	}{
		{"https://shop.example.com/list?color=red&size=l&brand=acme", PatternFaceted}, // three stacked facets
		{"https://shop.example.com/list?color=red&size=l", PatternNone},               // two is a real listing
		{"https://shop.example.com/list?sort=price", PatternNone},                     // one selector
		{"https://shop.example.com/list?color=red&size=l&brand=acme&material=wool&rating=4", PatternFaceted},
		{"https://shop.example.com/list?page=3", PatternNone}, // pagination is not a facet
	}
	for _, c := range cases {
		if got := d.Violation(c.url, now); got != c.want {
			t.Errorf("Violation(%q) = %v, want %v", c.url, got, c.want)
		}
	}
}

// TestViolationSession checks the session rule fires on a session-named parameter
// carrying a high-entropy token and ignores short or word-like values.
func TestViolationSession(t *testing.T) {
	d := NewTrapDetector()
	now := hoursAt(2026, 6, 1)
	cases := []struct {
		url  string
		want PatternKind
	}{
		{"https://x.example.net/p?phpsessid=9af3b1c2d4e5f60718293a4b5c6d7e8f", PatternSession},
		{"https://x.example.net/p?jsessionid=A1B2C3D4E5F60718293A4B5C6D7E8F90", PatternSession},
		{"https://x.example.net/p?sid=42", PatternNone},          // a short selector, not a session id
		{"https://x.example.net/p?sort=newest", PatternNone},     // not a session key
		{"https://x.example.net/p?sess=newsletter", PatternNone}, // a readable word, not a token
	}
	for _, c := range cases {
		if got := d.Violation(c.url, now); got != c.want {
			t.Errorf("Violation(%q) = %v, want %v", c.url, got, c.want)
		}
	}
}

// TestHighEntropyToken pins the opaque-token heuristic the session rule rests on.
func TestHighEntropyToken(t *testing.T) {
	yes := []string{
		"9af3b1c2d4e5f607", "A1B2C3D4E5F60718293A4B5C6D7E8F90",
		"abc123def456ghi789", "f00ba12bazqux99",
	}
	no := []string{
		"42", "newest", "short", "newsletter-signup", // word-like or too short
		"123456789012",   // all digits, a counter not an id
		"abcdefghijklmn", // all letters, a word not an id
		"has a space xx", // not a token
	}
	for _, v := range yes {
		if !highEntropyToken(v) {
			t.Errorf("highEntropyToken(%q) = false, want true", v)
		}
	}
	for _, v := range no {
		if highEntropyToken(v) {
			t.Errorf("highEntropyToken(%q) = true, want false", v)
		}
	}
}

// TestObserveFlagsHost feeds a stream of distinct future-dated URLs and checks the
// host crosses into suspect exactly at the threshold, with newlySuspect raised once.
func TestObserveFlagsHost(t *testing.T) {
	d := NewTrapDetector(WithFlagThreshold(8))
	now := hoursAt(2026, 6, 1)
	hk, _ := trapKey(t, "https://cal.example.org/2031/01/")

	flips := 0
	for i := range 12 {
		canon := fmt.Sprintf("https://cal.example.org/2031/%02d/", i+1) // distinct future months
		_, key := trapKey(t, canon)
		kind, newly := d.Observe(hk, key, canon, now)
		if kind != PatternCalendar {
			t.Fatalf("month %d: kind = %v, want calendar", i+1, kind)
		}
		if newly {
			flips++
			if i+1 != 8 {
				t.Fatalf("host flipped to suspect at %d distinct URLs, want at the threshold 8", i+1)
			}
		}
	}
	if flips != 1 {
		t.Fatalf("newlySuspect raised %d times, want exactly once", flips)
	}
	if !d.Suspect(hk) {
		t.Fatal("host not a suspect after crossing the threshold")
	}
}

// TestObserveDedupsRepeatedURL checks the same trap URL rediscovered does not
// inflate the count toward the threshold, so a host is flagged on distinct trap
// URLs, not on repeated delivery of one.
func TestObserveDedupsRepeatedURL(t *testing.T) {
	d := NewTrapDetector(WithFlagThreshold(4))
	now := hoursAt(2026, 6, 1)
	canon := "https://cal.example.org/2031/07/"
	hk, key := trapKey(t, canon)

	for range 20 {
		if _, newly := d.Observe(hk, key, canon, now); newly {
			t.Fatal("one URL delivered many times wrongly flagged the host")
		}
	}
	if d.Suspect(hk) {
		t.Fatal("host flagged on a single repeated URL")
	}
}

// TestPassesAppliesFlaggedRuleOnly checks the admission heuristic: an unflagged
// host passes everything; a host flagged only for calendar parks its future-dated
// URLs but still admits its real articles and even its facet URLs, because the
// detector applies only the rules the host was flagged for (doc 08, section 8.4).
func TestPassesAppliesFlaggedRuleOnly(t *testing.T) {
	d := NewTrapDetector(WithFlagThreshold(8))
	now := hoursAt(2026, 6, 1)
	hk, _ := trapKey(t, "https://cal.example.org/2031/01/")

	// An unflagged host passes a future-dated URL.
	future := "https://cal.example.org/2031/07/"
	if !d.Passes(hk, future, now) {
		t.Fatal("unflagged host parked a URL")
	}

	// Flag it for calendar with a stream of distinct future months.
	for i := range 8 {
		canon := fmt.Sprintf("https://cal.example.org/2031/%02d/", i+1)
		_, key := trapKey(t, canon)
		d.Observe(hk, key, canon, now)
	}
	if !d.Suspect(hk) {
		t.Fatal("host not flagged after the stream")
	}

	// Now the future-dated URL is parked, the real article is admitted.
	if d.Passes(hk, future, now) {
		t.Fatal("calendar-flagged host admitted a future-dated URL")
	}
	if !d.Passes(hk, "https://cal.example.org/2026/06/the-keynote", now) {
		t.Fatal("calendar-flagged host parked a within-horizon URL")
	}
	if !d.Passes(hk, "https://cal.example.org/about/contact", now) {
		t.Fatal("calendar-flagged host parked a real article URL")
	}
	// It was not flagged for facets, so a facet URL still passes the calendar host.
	if !d.Passes(hk, "https://cal.example.org/list?color=red&size=l&brand=acme", now) {
		t.Fatal("calendar-only flag wrongly parked a facet URL")
	}
}

// TestCorpusTrapPrecision is the real-data precision gate (doc 08, section 8.4, the
// clean-corpus side of the heuristic). The frozen ccrawl slice is a healthy crawl,
// so the two structural rules that are easy to trip on honest content, calendar and
// facet, must flag no host: a date misread from an id or a real filtered listing
// must never raise a flag. The session rule is allowed to fire, because the slice
// does contain one genuine session-token explosion (one discussions page served
// under hundreds of distinct ids), and parking those redundant variants is the
// right call; the gate confirms any session flag is backed by a real explosion (one
// base path carrying at least the threshold of distinct session URLs), so it is a
// true positive, not a false one. Recall against a full link farm is the doc 14
// follow-up that wants a farm slice to exercise on.
func TestCorpusTrapPrecision(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	urls := loadCorpusURLs(t, path)
	if len(urls) == 0 {
		t.Fatalf("corpus %s produced no canonical URLs", path)
	}

	// A fixed campaign clock so the calendar horizon is deterministic; the slice is
	// CC-MAIN-2026-25, so mid-2026 is the right "now".
	now := hoursAt(2026, 6, 1)
	d := NewTrapDetector()

	flagged := map[uint64]struct{}{}
	perKind := map[PatternKind]int{}
	// An independent count of distinct session URLs per (host, base path), to confirm
	// a session flag is a genuine explosion rather than the detector's own claim.
	sessionByPath := map[uint64]map[string]map[string]struct{}{}
	for _, u := range urls {
		key, _, ok := Key(u, meguri.GroupRegistrableDomain)
		if !ok {
			continue
		}
		kind, newly := d.Observe(key.HostKey, key, u, now)
		if kind != PatternNone {
			perKind[kind]++
		}
		if kind == PatternSession {
			base := basePath(u)
			if sessionByPath[key.HostKey] == nil {
				sessionByPath[key.HostKey] = map[string]map[string]struct{}{}
			}
			if sessionByPath[key.HostKey][base] == nil {
				sessionByPath[key.HostKey][base] = map[string]struct{}{}
			}
			sessionByPath[key.HostKey][base][u] = struct{}{}
		}
		if newly {
			flagged[key.HostKey] = struct{}{}
		}
	}

	sessionFlags := 0
	for hk := range flagged {
		cal, fac, ses := d.Flags(hk)
		if cal || fac {
			t.Fatalf("host %#x flagged on a structural rule (calendar=%v faceted=%v) on a clean corpus", hk, cal, fac)
		}
		if ses {
			sessionFlags++
			// Confirm a real explosion backs the flag: one base path under many ids.
			maxDistinct := 0
			for _, ids := range sessionByPath[hk] {
				maxDistinct = max(maxDistinct, len(ids))
			}
			if maxDistinct < defaultFlagThreshold {
				t.Fatalf("host %#x flagged for session but its busiest base path has only %d distinct session URLs, not a real explosion", hk, maxDistinct)
			}
		}
	}
	t.Logf("corpus trap precision: %d urls, %d hosts flagged (all session, each a confirmed token explosion); signature hits calendar=%d faceted=%d session=%d",
		len(urls), sessionFlags, perKind[PatternCalendar], perKind[PatternFaceted], perKind[PatternSession])
}
