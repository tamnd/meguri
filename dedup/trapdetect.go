package dedup

import (
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/tamnd/meguri"
)

// PatternKind names the trap signature a URL bears, the structural tell that a
// host is generating frontier entries faster than it serves distinct content
// (doc 08, section 8.4). PatternNone is a URL with no trap signature.
type PatternKind uint8

const (
	PatternNone     PatternKind = iota
	PatternCalendar             // an advancing date past the crawl horizon, the infinite-calendar walk
	PatternFaceted              // stacked filter parameters, the combinatorial facet explosion
	PatternSession              // a high-entropy session id, a fresh identity per visit for one page
)

// String renders a PatternKind for logs and test failures.
func (k PatternKind) String() string {
	switch k {
	case PatternCalendar:
		return "calendar"
	case PatternFaceted:
		return "faceted"
	case PatternSession:
		return "session"
	default:
		return "none"
	}
}

const (
	// defaultHorizonMonths is how far past the current month a calendar URL may
	// point before it is a trap. A year of lookahead admits a real upcoming-events
	// page while parking the calendar walk that marches into the next decade.
	defaultHorizonMonths = 12
	// defaultFacetLimit is how many stacked facet parameters mark a URL as a
	// combinatorial facet trap. Zero, one, or two facets is a real filtered
	// listing; three or more stacked facets is the cartesian product a facet farm
	// generates (doc 08, section 8.4).
	defaultFacetLimit = 3
	// defaultFlagThreshold is how many distinct trap-signature URLs of one kind a
	// host must emit before it is flagged a trap suspect. A handful of dated or
	// filtered URLs is normal; a stream of them is the trap. The threshold is per
	// kind, so calendar, facet, and session signals do not pool.
	defaultFlagThreshold = 16
	// sessionMinLen is the shortest query value that can be a session id. Below it
	// a value is a real selector (a page number, a short slug), not the opaque
	// high-entropy token a session id is.
	sessionMinLen = 12
)

// facetParams are the query keys that select a facet of a listing rather than a
// distinct resource: a stack of them is the same catalog narrowed, not new pages
// (doc 08, section 8.4). Pagination keys are deliberately absent, so a healthy
// paginated listing is never mistaken for a facet farm.
var facetParams = map[string]bool{
	"filter": true, "facet": true, "refine": true, "narrow": true,
	"color": true, "colour": true, "size": true, "brand": true,
	"price": true, "price_min": true, "price_max": true, "minprice": true, "maxprice": true,
	"pricerange": true, "sort": true, "sortby": true, "order": true, "orderby": true,
	"view": true, "mode": true, "layout": true, "category": true, "cat": true,
	"tag": true, "tags": true, "style": true, "material": true, "rating": true,
	"availability": true, "instock": true, "attr": true, "option": true,
}

// sessionParams are the query keys that carry a session id: a value under one of
// these that is a long opaque token is a per-visit identity for one resource, not
// a selector (doc 08, section 8.4). Ambiguous short keys (a bare s, q, id) are
// left out, and the value must still look high-entropy, so a real ?id=42 never
// trips the rule.
var sessionParams = map[string]bool{
	"sid": true, "sessionid": true, "session_id": true, "sessid": true,
	"sess": true, "jsessionid": true, "phpsessid": true, "aspsessionid": true,
	"aspxauth": true, "cfid": true, "cftoken": true, "sessiontoken": true,
	"session_token": true, "sessionkey": true, "bsessionid": true, "zenid": true,
	"oscsid": true, "ssid": true,
}

// dateQueryKeys are the query keys that carry a calendar date the calendar rule
// reads, beyond the year/month pair a path like /2031/07/ encodes.
var dateQueryKeys = map[string]bool{
	"date": true, "day": true, "month": true, "year": true,
	"from": true, "to": true, "start": true, "end": true, "when": true,
}

// TrapDetector recognizes calendar, faceted, and session-id trap signatures in a
// host's URL stream and decides, for a flagged host, whether a single discovery
// passes the pattern heuristics (doc 08, section 8.4). It is the precise defense
// layered on the blunt per-host budget and depth cap: the cap bounds every host,
// and the detector catches the trap hosts earlier and tighter.
//
// The detector is pure given its inputs: the only clock it sees is the epoch-hours
// the caller threads in, which it converts to a year-month to place the calendar
// horizon. Same URLs, same now, same verdict, so a checkpoint replay is exact.
//
// It accumulates per host: each kind has its own count of distinct trap-signature
// URLs, and a host crosses into suspect for a kind when that count passes the
// threshold. Suspicion is sticky for the life of the detector; the HostRecord flag
// the caller sets from it is what survives a checkpoint (doc 08, section 8.4).
type TrapDetector struct {
	horizonMonths int
	facetLimit    int
	flagThreshold int

	hosts map[uint64]*hostTrap
}

// hostTrap is the per-host accumulation. Calendar and facet count distinct
// signature URLs across the host: a stream of future-dated or heavily-faceted
// URLs is the host-wide tell. Session counts distinct tokens per base path
// instead, because the session tell is one resource served under many ids, not
// many session URLs scattered across a host, so a host that shows one session id
// on each of many real pages is not a session trap but one page under a thousand
// ids is.
type hostTrap struct {
	cal, fac int            // distinct calendar and facet signature URLs on the host
	session  map[string]int // base path (no query) -> distinct session-token URLs on it
	flagged  [4]bool        // kinds this host is a confirmed suspect for
	// seen dedups the signature URLs so the same trap URL rediscovered does not
	// inflate the count toward the threshold twice.
	seen map[meguri.URLKey]struct{}
}

// TrapOption configures a TrapDetector.
type TrapOption func(*TrapDetector)

// WithCalendarHorizon sets how many months past the current month a dated URL may
// point before the calendar rule parks it.
func WithCalendarHorizon(months int) TrapOption {
	return func(d *TrapDetector) {
		if months > 0 {
			d.horizonMonths = months
		}
	}
}

// WithFacetLimit sets how many stacked facet parameters mark a faceted trap.
func WithFacetLimit(n int) TrapOption {
	return func(d *TrapDetector) {
		if n > 0 {
			d.facetLimit = n
		}
	}
}

// WithFlagThreshold sets how many distinct trap-signature URLs of one kind flag a
// host a suspect for that kind.
func WithFlagThreshold(n int) TrapOption {
	return func(d *TrapDetector) {
		if n > 0 {
			d.flagThreshold = n
		}
	}
}

// NewTrapDetector returns a detector at the default thresholds (doc 09 may retune
// them per campaign).
func NewTrapDetector(opts ...TrapOption) *TrapDetector {
	d := &TrapDetector{
		horizonMonths: defaultHorizonMonths,
		facetLimit:    defaultFacetLimit,
		flagThreshold: defaultFlagThreshold,
		hosts:         make(map[uint64]*hostTrap),
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Violation classifies a single canonical URL: it returns the trap signature the
// URL bears, or PatternNone. It is the pure per-URL rule both the accumulation and
// the admission test are built on, so a URL the detector would count toward a flag
// is exactly a URL the detector would park on a flagged host. now is epoch-hours,
// the campaign clock the calendar horizon is measured from.
//
// The order is calendar, then session, then faceted: a URL that both carries a
// session id and stacks facets is reported by its strongest signal first, but
// every rule is checked independently on a flagged host, so the order only names
// the URL, it does not weaken any rule.
func (d *TrapDetector) Violation(canonURL string, now uint32) PatternKind {
	u, err := url.Parse(canonURL)
	if err != nil {
		return PatternNone
	}
	q := u.Query()
	if d.calendarViolation(u, q, now) {
		return PatternCalendar
	}
	if sessionViolation(q) {
		return PatternSession
	}
	if d.facetViolation(q) {
		return PatternFaceted
	}
	return PatternNone
}

// Observe folds one discovery into the host's accumulation and reports the URL's
// trap signature and whether the host became a suspect (for any kind) on this
// observation, so the caller flags the HostRecord exactly once. A URL with no trap
// signature still counts as an observation but moves no counter.
//
// The host key the URL belongs to is passed explicitly rather than re-derived, so
// the detector shares the frontier's host grouping without re-parsing.
func (d *TrapDetector) Observe(hostKey uint64, key meguri.URLKey, canonURL string, now uint32) (PatternKind, bool) {
	kind := d.Violation(canonURL, now)
	if kind == PatternNone {
		return PatternNone, false
	}

	ht := d.hosts[hostKey]
	if ht == nil {
		ht = &hostTrap{session: make(map[string]int), seen: make(map[meguri.URLKey]struct{})}
		d.hosts[hostKey] = ht
	}
	if _, dup := ht.seen[key]; dup {
		return kind, false // already counted this trap URL, do not double-count
	}
	ht.seen[key] = struct{}{}

	wasSuspect := ht.anyFlagged()
	switch kind {
	case PatternCalendar:
		ht.cal++
		if ht.cal >= d.flagThreshold {
			ht.flagged[PatternCalendar] = true
		}
	case PatternFaceted:
		ht.fac++
		if ht.fac >= d.flagThreshold {
			ht.flagged[PatternFaceted] = true
		}
	case PatternSession:
		base := basePath(canonURL)
		ht.session[base]++
		if ht.session[base] >= d.flagThreshold {
			ht.flagged[PatternSession] = true
		}
	}
	return kind, ht.anyFlagged() && !wasSuspect
}

// basePath is the canonical URL with its query stripped, the resource a session id
// decorates. Counting distinct session URLs per base path is what separates one
// page served under a thousand session ids (a trap) from a thousand pages each
// carrying one (not a trap).
func basePath(canonURL string) string {
	base, _, _ := strings.Cut(canonURL, "?")
	return base
}

// Suspect reports whether the host has crossed the threshold on any trap kind.
func (d *TrapDetector) Suspect(hostKey uint64) bool {
	ht := d.hosts[hostKey]
	return ht != nil && ht.anyFlagged()
}

// Flags reports which trap kinds the host is flagged for, so a caller can see why
// a host became a suspect (and a precision gate can tell a structural calendar or
// facet flag from a session-token explosion).
func (d *TrapDetector) Flags(hostKey uint64) (calendar, faceted, session bool) {
	ht := d.hosts[hostKey]
	if ht == nil {
		return false, false, false
	}
	return ht.flagged[PatternCalendar], ht.flagged[PatternFaceted], ht.flagged[PatternSession]
}

// Passes is the admission heuristic feeding Admit's passesHeuristics argument: it
// reports whether a discovery on a flagged host may be scheduled. A host that is
// not a suspect passes everything (admission is unchanged until a host is flagged).
// A flagged host passes a URL only when the URL does not violate one of the rules
// the host was flagged for, so the calendar URLs of a calendar-flagged host are
// parked while its real article URLs are admitted (doc 08, section 8.4).
func (d *TrapDetector) Passes(hostKey uint64, canonURL string, now uint32) bool {
	ht := d.hosts[hostKey]
	if ht == nil || !ht.anyFlagged() {
		return true
	}
	u, err := url.Parse(canonURL)
	if err != nil {
		return true // an unparseable URL is not the trap pattern; leave it to the blunt cap
	}
	q := u.Query()
	if ht.flagged[PatternCalendar] && d.calendarViolation(u, q, now) {
		return false
	}
	if ht.flagged[PatternSession] && sessionViolation(q) {
		return false
	}
	if ht.flagged[PatternFaceted] && d.facetViolation(q) {
		return false
	}
	return true
}

func (ht *hostTrap) anyFlagged() bool {
	return ht.flagged[PatternCalendar] || ht.flagged[PatternFaceted] || ht.flagged[PatternSession]
}

// calendarViolation reports whether the URL points to a month past the crawl
// horizon, the future date that marks the infinite-calendar walk. A URL with no
// date, or a past or near-future date, is not a violation, so a real archive of
// dated articles and a real upcoming-events page within the horizon both pass; the
// trap is the date that keeps advancing past the horizon (doc 08, section 10.3).
func (d *TrapDetector) calendarViolation(u *url.URL, q url.Values, now uint32) bool {
	months, ok := urlMonths(u.Path, q)
	if !ok {
		return false
	}
	return months > nowMonths(now)+d.horizonMonths
}

// facetViolation reports whether the query stacks at least facetLimit facet
// parameters, the combinatorial signal a facet farm leaves. Below the limit the
// URL is a real filtered listing, so it survives facet stripping with identity to
// spare; at or above it the URL is one cell of the cartesian product (doc 08,
// section 8.4).
func (d *TrapDetector) facetViolation(q url.Values) bool {
	n := 0
	for k := range q {
		if facetParams[strings.ToLower(k)] {
			n++
		}
	}
	return n >= d.facetLimit
}

// sessionViolation reports whether the query carries a session-id parameter with a
// high-entropy value, the per-visit identity a session trap mints for one page. A
// session key with a short or word-like value is a real selector and passes.
func sessionViolation(q url.Values) bool {
	for k, vs := range q {
		if !sessionParams[strings.ToLower(k)] {
			continue
		}
		if slices.ContainsFunc(vs, highEntropyToken) {
			return true
		}
	}
	return false
}

// urlMonths extracts a year-month from a URL as a count of months since year 0, so
// two dates compare as integers. It reads a /YYYY/MM path pair first (the common
// calendar path form), then falls back to year and month query parameters or a
// date=YYYY-MM-DD value. ok is false when no date is present.
func urlMonths(path string, q url.Values) (int, bool) {
	if y, m, ok := pathYearMonth(path); ok {
		return y*12 + (m - 1), true
	}
	return queryYearMonth(q)
}

// pathYearMonth scans the path segments for a four-digit year immediately followed
// by a one or two digit month, the /2031/07/ calendar form. The adjacent month is
// required: a lone numeric segment like /issues/2145 or /pull/2187 is an id, not a
// date, and reading it as a year is the false positive a clean corpus exposes. A
// real calendar always names the month it navigates to, so requiring the pair
// keeps recall while dropping the bare-number ids.
func pathYearMonth(path string) (year, month int, ok bool) {
	segs := strings.Split(path, "/")
	for i, s := range segs {
		y, err := strconv.Atoi(s)
		if err != nil || !plausibleYear(y) {
			continue
		}
		if i+1 >= len(segs) {
			continue
		}
		m, err := strconv.Atoi(segs[i+1])
		if err != nil || m < 1 || m > 12 {
			continue
		}
		return y, m, true
	}
	return 0, 0, false
}

// queryYearMonth reads a date from the query: a year (with an optional month), or a
// date= value in YYYY-MM-DD or YYYY/MM form.
func queryYearMonth(q url.Values) (int, bool) {
	if !hasDateKey(q) {
		return 0, false
	}
	if ys := q.Get("year"); ys != "" {
		if y, err := strconv.Atoi(ys); err == nil && plausibleYear(y) {
			month := 1
			if m, err := strconv.Atoi(q.Get("month")); err == nil && m >= 1 && m <= 12 {
				month = m
			}
			return y*12 + (month - 1), true
		}
	}
	// A date-bearing value (date=, from=, when=, and the like) in YYYY-MM(-DD) form.
	for key, vs := range q {
		lk := strings.ToLower(key)
		if !dateQueryKeys[lk] || lk == "year" || lk == "month" {
			continue
		}
		for _, v := range vs {
			if y, m, ok := parseDateValue(v); ok {
				return y*12 + (m - 1), true
			}
		}
	}
	return 0, false
}

func hasDateKey(q url.Values) bool {
	for k := range q {
		if dateQueryKeys[strings.ToLower(k)] {
			return true
		}
	}
	return false
}

// parseDateValue reads a YYYY-MM or YYYY-MM-DD (or slash-separated) date value,
// returning its year and month.
func parseDateValue(v string) (year, month int, ok bool) {
	v = strings.TrimSpace(v)
	sep := byte('-')
	if !strings.ContainsRune(v, '-') && strings.ContainsRune(v, '/') {
		sep = '/'
	}
	parts := strings.Split(v, string(sep))
	if len(parts) < 2 {
		return 0, 0, false
	}
	y, err := strconv.Atoi(parts[0])
	if err != nil || !plausibleYear(y) {
		return 0, 0, false
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 1 || m > 12 {
		return 0, 0, false
	}
	return y, m, true
}

// plausibleYear bounds what counts as a calendar year, so a four-digit product id
// or a port number is not read as a date. The range spans a healthy archive's past
// and a calendar trap's runaway future without admitting arbitrary integers; the
// 2099 ceiling keeps four-digit ids above it (an issue or pull number like 2145)
// from being read as years.
func plausibleYear(y int) bool {
	return y >= 1990 && y <= 2099
}

// nowMonths converts epoch-hours to a count of months since year 0, the same scale
// urlMonths returns, so the horizon comparison is a single integer add. It is a
// pure civil-date conversion (the days-to-year-month-day algorithm), no clock and
// no allocation.
func nowMonths(epochHours uint32) int {
	days := int(int64(epochHours) * 3600 / 86400)
	y, m, _ := civilFromDays(days)
	return y*12 + (m - 1)
}

// civilFromDays converts a day count since the Unix epoch to a calendar year,
// month, and day. It is Howard Hinnant's branchless civil-from-days algorithm,
// exact across the whole range and free of any library clock.
func civilFromDays(z int) (year, month, day int) {
	z += 719468
	era := z
	if z < 0 {
		era = z - 146096
	}
	era /= 146097
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y := yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	day = doy - (153*mp+2)/5 + 1
	month = mp + 3
	if mp >= 10 {
		month = mp - 9
	}
	if month <= 2 {
		y++
	}
	return y, month, day
}

// highEntropyToken reports whether a value looks like an opaque session id rather
// than a human-meaningful selector: long enough, drawn from the token alphabet,
// and mixing letters and digits the way a generated id does. A page number, a
// short slug, or a readable word fails, so a real selector under a session-named
// key is not mistaken for a session trap.
func highEntropyToken(v string) bool {
	if len(v) < sessionMinLen {
		return false
	}
	var letters, digits, other int
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
			letters++
		case c >= '0' && c <= '9':
			digits++
		case c == '-', c == '_', c == '.', c == '%':
			other++
		default:
			return false // a space or punctuation: not an opaque token
		}
	}
	// A long pure-hex or pure-alnum token with both letters and digits is the
	// generated-id shape; a value that is all letters (a long word) or all digits
	// (a timestamp or counter) is a real selector, not a session id.
	return letters >= 2 && digits >= 1 && other <= len(v)/4
}
