package frontier

import (
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/prioritize"
)

// disc builds a routed out-link to url on host, carrying the OPIC cash and the
// source host the prioritizer reads.
func disc(url, host string, srcHost uint64) meguri.Discovery {
	return meguri.Discovery{
		URLKey:          keyFor(host, url),
		CanonicalURL:    url,
		Depth:           1,
		SrcHostKey:      srcHost,
		DiscoverySource: meguri.SourceLink,
	}
}

// reportLinks reports a crawl of a source URL whose body extracted links, the
// event that drives the OPIC cash distribution.
func reportLinks(f *Frontier, key meguri.URLKey, hour uint32, links []meguri.Discovery) {
	f.Report(meguri.Outcome{
		URLKey:     key,
		HTTPStatus: 200,
		FetchedAt:  hour,
		ContentFP:  uint64(hour) + 1,
		Links:      links,
	}, hour*3600)
}

// TestPrioritizationOffKeepsLinkWeight pins the opt-in contract: with the
// prioritizer off, a seed keeps the caller's flat priority and a discovered link
// keeps its raw link weight, the M1 and M2 behavior, and a crawl never ingests
// its out-links.
func TestPrioritizationOffKeepsLinkWeight(t *testing.T) {
	f := New(1, 0)
	f.Seed("http://a.test/x", "a.test", 0.5, 0, 0, 10)
	if got := f.records[keyFor("a.test", "http://a.test/x")].Priority; got != 0.5 {
		t.Errorf("seed priority = %v, want the flat 0.5", got)
	}

	f.Discover(meguri.Discovery{
		URLKey:       keyFor("b.test", "http://b.test/y"),
		CanonicalURL: "http://b.test/y",
		LinkWeight:   0.7,
		SrcHostKey:   meguri.HostKeyOf("a.test"),
	}, 0)
	if got := f.records[keyFor("b.test", "http://b.test/y")].Priority; got != 0.7 {
		t.Errorf("discovered priority = %v, want the raw link weight 0.7", got)
	}

	// A crawl with extracted links does not ingest them when prioritization is off.
	before := f.Len()
	reportLinks(f, keyFor("a.test", "http://a.test/x"), 1, []meguri.Discovery{disc("http://c.test/z", "c.test", meguri.HostKeyOf("a.test"))})
	if f.Len() != before {
		t.Errorf("a crawl ingested links with the prioritizer off: len %d -> %d", before, f.Len())
	}
}

// TestOPICOrdersByInDegree is the central OPIC claim wired end to end (doc 09): a
// URL three crawled pages link to earns a higher priority than a URL one page
// links to, with no graph computation, just cash flowing from the crawled sources
// to their out-links.
func TestOPICOrdersByInDegree(t *testing.T) {
	f := New(1, 0, WithPrioritizer(prioritize.DefaultParams()))
	srcs := []struct{ url, host string }{
		{"http://s1.test/", "s1.test"},
		{"http://s2.test/", "s2.test"},
		{"http://s3.test/", "s3.test"},
	}
	const hub = "http://hub.test/page"
	const lonely = "http://lonely.test/page"
	for _, s := range srcs {
		f.Seed(s.url, s.host, 1.0, 0, 0, 10)
	}
	// Each source links to the hub; only the first also links to the lonely page.
	for i, s := range srcs {
		links := []meguri.Discovery{disc(hub, "hub.test", meguri.HostKeyOf(s.host))}
		if i == 0 {
			links = append(links, disc(lonely, "lonely.test", meguri.HostKeyOf(s.host)))
		}
		reportLinks(f, keyFor(s.host, s.url), uint32(i+1), links)
	}

	hubRec := f.records[keyFor("hub.test", hub)]
	lonelyRec := f.records[keyFor("lonely.test", lonely)]
	if hubRec == nil || lonelyRec == nil {
		t.Fatal("the crawl did not ingest its out-links")
	}
	if !(hubRec.Priority > lonelyRec.Priority) {
		t.Fatalf("OPIC did not order by in-degree: hub=%v lonely=%v", hubRec.Priority, lonelyRec.Priority)
	}
}

// TestSTARBudgetDefersSpamFarm is the STAR-BEAST guarantee wired end to end
// (doc 09): a host flooded with its own internal links earns no cross-host
// reputation, so it gets the floor budget and its over-budget URLs are parked in
// Trapped, while a host many distinct other domains link to earns a far larger
// budget.
func TestSTARBudgetDefersSpamFarm(t *testing.T) {
	p := prioritize.DefaultParams()
	p.BaseBudget = 8
	p.MinBudget = 8
	p.PerInLink = 8
	f := New(1, 0, WithPrioritizer(p))

	spamHost := meguri.HostKeyOf("spam.test")
	var trapped, scheduled int
	for i := range 100 {
		url := "http://spam.test/p" + itoa(i)
		// Every link is internal to the farm: src host == target host.
		d := disc(url, "spam.test", spamHost)
		f.Discover(d, 0)
		switch f.records[keyFor("spam.test", url)].Status {
		case meguri.StatusTrapped:
			trapped++
		case meguri.StatusScheduled:
			scheduled++
		}
	}
	if trapped == 0 {
		t.Fatalf("the spam farm was never deferred: %d scheduled, %d trapped", scheduled, trapped)
	}
	spamBudget := f.hosts[spamHost].rec.URLBudget

	// A reputable host: each link arrives from a distinct other domain.
	goodHost := meguri.HostKeyOf("good.test")
	for i := range 50 {
		url := "http://good.test/p" + itoa(i)
		d := disc(url, "good.test", meguri.HostKeyOf("ref"+itoa(i)+".test"))
		f.Discover(d, 0)
	}
	goodBudget := f.hosts[goodHost].rec.URLBudget
	if goodBudget <= spamBudget {
		t.Fatalf("reputation did not earn budget: good=%d spam=%d", goodBudget, spamBudget)
	}
}
