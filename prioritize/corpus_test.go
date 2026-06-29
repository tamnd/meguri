package prioritize

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/tamnd/meguri"
)

// This file is the M5 gate on real data (doc 09, doc 14): the OPIC estimator, the
// STAR cross-host budget, and the depth penalty are exercised over the frozen
// ccrawl corpus (CC-MAIN-2026-25, the tamnd/ccrawl-cli slice pinned in corpus/).
// Three real signals carry the gate.
//
// First, the real redirect graph. Every 3xx row carries the URL it redirected to,
// a real observed directed link from one page to another. The pinned slice holds
// ~20k such edges over ~19.7k distinct targets, and a target's in-degree is the
// distinct count of pages that redirect to it (max 134 here). That is the in-link
// structure OPIC turns into importance: cash flows from each crawled source to its
// out-link, so a target many pages point at accumulates more cash than a target
// one page points at, with no graph computation.
//
// Second, the real cross-host edges. Where a redirect crosses host groups it is a
// forgery-resistant reputation signal, the STAR input that sets a host's budget.
//
// Third, the real path depth. The URL path segment count spans 0 to past 11 across
// the slice, the distribution the depth penalty tilts crawling toward the shallow,
// high-value pages over deep ones.
//
// The honesty flag (D19, doc 14): a redirect is only one kind of link, and in a
// curated developer-docs slice most redirects are intra-host (http to https, a
// trailing slash), so the cross-host reputation signal is real but sparse (19
// hosts). The full page out-link graph, which ami extracts from WAT, is the richer
// signal and is the doc 14 follow-up; this gate uses the redirect graph the pinned
// slice already holds. The slice is also a clean set of real authorities with no
// adversarial link farm, so the trap penalty (D17) is gated by the unit tests
// rather than invented onto a real host here.

// edge is one observed redirect link, src to dst.
type edge struct{ src, dst string }

// corpusGraph is the redirect graph and the per-URL depth read from the pinned
// slice, the inputs the three M5 gates share.
type corpusGraph struct {
	edges []edge            // distinct redirect edges
	depth map[string]int    // path depth per distinct URL
	hosts map[string]uint64 // host string to host key, for the STAR gate
}

// loadGraph reads the pinned ccrawl jsonl slice and returns the deduplicated
// redirect graph plus each distinct URL's path depth. It is self-contained so the
// prioritize gate does not lean on the freshness or frontier test helpers.
func loadGraph(tb testing.TB, path string) corpusGraph {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type row struct {
		URL      string `json:"url"`
		Redirect string `json:"redirect"`
	}
	seenEdge := map[edge]struct{}{}
	g := corpusGraph{depth: map[string]int{}, hosts: map[string]uint64{}}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r row
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		g.depth[r.URL] = pathDepth(r.URL)
		if h := hostOf(r.URL); h != "" {
			g.hosts[h] = meguri.HostKeyOf(h)
		}
		if r.Redirect == "" {
			continue
		}
		g.depth[r.Redirect] = pathDepth(r.Redirect)
		if h := hostOf(r.Redirect); h != "" {
			g.hosts[h] = meguri.HostKeyOf(h)
		}
		e := edge{src: r.URL, dst: r.Redirect}
		if _, ok := seenEdge[e]; ok {
			continue
		}
		seenEdge[e] = struct{}{}
		g.edges = append(g.edges, e)
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	sort.Slice(g.edges, func(i, j int) bool {
		if g.edges[i].src != g.edges[j].src {
			return g.edges[i].src < g.edges[j].src
		}
		return g.edges[i].dst < g.edges[j].dst
	})
	return g
}

// TestCorpusOPICRanksByRedirectInDegree is the OPIC gate on the real redirect
// graph (doc 09): seed every source page with equal cash, let one crawl spread it
// across each page's redirect link, and the targets many pages redirect to must
// outscore the targets one page redirects to, purely from accumulated cash.
func TestCorpusOPICRanksByRedirectInDegree(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	p := DefaultParams()
	g := loadGraph(t, path)

	o := NewOPIC(p)
	// Distinct in-degree per target: how many distinct source pages redirect to it.
	inDegree := map[meguri.URLKey]int{}
	srcSeeded := map[string]bool{}
	for _, e := range g.edges {
		sk := urlKey(e.src)
		if !srcSeeded[e.src] {
			o.Seed(sk, 1) // equal starting endowment per source page
			srcSeeded[e.src] = true
		}
		inDegree[urlKey(e.dst)]++
	}
	// One crawl round: each seeded source spends its cash across its single link.
	for _, e := range g.edges {
		links := []meguri.Discovery{{URLKey: urlKey(e.dst), SrcHostKey: meguri.HostKeyOf(hostOf(e.src))}}
		o.Distribute(urlKey(e.src), links)
		o.Credit(links[0].URLKey, links[0].LinkWeight)
	}

	var multiSum, singleSum float64
	var multiN, singleN int
	for k, d := range inDegree {
		s := float64(o.Score(k))
		if d >= 2 {
			multiSum += s
			multiN++
		} else {
			singleSum += s
			singleN++
		}
	}
	// In-degree is the distinct count of source pages, so repeated captures of one
	// redirect do not inflate it; the deduplicated slice holds ~49 targets two or
	// more distinct pages redirect to, against ~19.7k single-source targets.
	if multiN < 40 || singleN < 1000 {
		t.Fatalf("redirect graph too thin: %d multi-in-degree targets, %d single", multiN, singleN)
	}
	multiMean := multiSum / float64(multiN)
	singleMean := singleSum / float64(singleN)
	t.Logf("OPIC gate: %d targets in-degree>=2 (mean score %.5f), %d in-degree==1 (mean score %.5f)", multiN, multiMean, singleN, singleMean)
	if !(multiMean > singleMean) {
		t.Fatalf("OPIC did not rank by real redirect in-degree: multi=%.5f single=%.5f", multiMean, singleMean)
	}
}

// TestCorpusSTARBudgetTracksCrossHostInDegree is the STAR gate on the real
// cross-host redirect edges (doc 09): a host other domains redirect into earns a
// larger budget than a host with no external reputation, and the budget rises
// monotonically with the distinct cross-host in-degree.
func TestCorpusSTARBudgetTracksCrossHostInDegree(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	p := DefaultParams()
	g := loadGraph(t, path)

	c := NewCrossHostInDegree()
	for _, e := range g.edges {
		sh, dh := hostOf(e.src), hostOf(e.dst)
		if sh == "" || dh == "" {
			continue
		}
		c.Observe(meguri.HostKeyOf(dh), meguri.HostKeyOf(sh)) // same-host links self-cancel inside Observe
	}

	// Budget every host from its real cross-host reputation, and check the budget is
	// monotone in the in-degree and that the most-linked host clears the floor.
	type hb struct {
		host   string
		indeg  uint32
		budget uint32
	}
	var rows []hb
	var withRep int
	for h, key := range g.hosts {
		indeg := c.Count(key)
		var rec meguri.HostRecord
		UpdateHostBudget(&rec, indeg, p)
		if indeg > 0 {
			withRep++
		}
		rows = append(rows, hb{host: h, indeg: indeg, budget: rec.URLBudget})
	}
	if withRep == 0 {
		t.Fatal("no cross-host redirect reputation found in the slice")
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].indeg < rows[j].indeg })
	for i := 1; i < len(rows); i++ {
		if rows[i].budget < rows[i-1].budget {
			t.Fatalf("budget not monotone in cross-host in-degree: %+v before %+v", rows[i-1], rows[i])
		}
	}
	top := rows[len(rows)-1]
	bottom := rows[0]
	t.Logf("STAR gate: %d hosts, %d with cross-host reputation; top=%s indeg=%d budget=%d, floor host=%s indeg=%d budget=%d", len(rows), withRep, top.host, top.indeg, top.budget, bottom.host, bottom.indeg, bottom.budget)
	if !(top.budget > bottom.budget) {
		t.Fatalf("reputation did not earn budget: top=%d floor=%d", top.budget, bottom.budget)
	}
	if bottom.budget < p.MinBudget {
		t.Fatalf("a host fell below the budget floor: %d < %d", bottom.budget, p.MinBudget)
	}
}

// TestCorpusDepthPenaltyTiltsShallow is the depth gate on the real path-depth
// distribution (doc 09): applying the depth penalty to a flat base priority across
// the real corpus must rate the shallow pages above the deep ones, the tilt that
// keeps a crawl on the high-value top of each site.
func TestCorpusDepthPenaltyTiltsShallow(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to the pinned ccrawl jsonl slice (corpus/urls.jsonl)")
	}
	p := DefaultParams()
	g := loadGraph(t, path)

	var shallowSum, deepSum float64
	var shallowN, deepN int
	for _, d := range g.depth {
		pri := float64(DepthPenalty(1.0, uint16(d), p))
		switch {
		case d <= 2:
			shallowSum += pri
			shallowN++
		case d >= 5:
			deepSum += pri
			deepN++
		}
	}
	if shallowN < 1000 || deepN < 1000 {
		t.Fatalf("depth distribution too thin: %d shallow, %d deep", shallowN, deepN)
	}
	shallowMean := shallowSum / float64(shallowN)
	deepMean := deepSum / float64(deepN)
	t.Logf("depth gate: %d shallow (depth<=2, mean priority %.5f), %d deep (depth>=5, mean priority %.5f)", shallowN, shallowMean, deepN, deepMean)
	if !(shallowMean > deepMean) {
		t.Fatalf("depth penalty did not tilt toward shallow pages: shallow=%.5f deep=%.5f", shallowMean, deepMean)
	}
}

// --- small deterministic helpers, no wall-clock or global random state ---

// urlKey maps a URL string to a stable URLKey: the host key from the registrable
// host and a content hash of the full URL for the path, enough to give the gate
// distinct keys on shared and distinct hosts.
func urlKey(u string) meguri.URLKey {
	return meguri.URLKey{HostKey: meguri.HostKeyOf(hostOf(u)), PathKey: fnv64(u)}
}

// hostOf extracts the host from an absolute http(s) URL without net/url.
func hostOf(u string) string {
	_, rest, ok := strings.Cut(u, "://")
	if !ok {
		return ""
	}
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.IndexByte(rest, '@'); k >= 0 {
		rest = rest[k+1:]
	}
	if k := strings.IndexByte(rest, ':'); k >= 0 {
		rest = rest[:k]
	}
	return rest
}

// pathDepth counts the non-empty path segments of a URL, the real depth signal.
func pathDepth(u string) int {
	_, rest, ok := strings.Cut(u, "://")
	if !ok {
		return 0
	}
	i := strings.IndexByte(rest, '/')
	if i < 0 {
		return 0
	}
	path := rest[i:]
	if j := strings.IndexAny(path, "?#"); j >= 0 {
		path = path[:j]
	}
	var n int
	for seg := range strings.SplitSeq(path, "/") {
		if seg != "" {
			n++
		}
	}
	return n
}

func fnv64(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}
