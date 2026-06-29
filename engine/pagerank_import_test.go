package engine

import (
	"math"
	"os"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/distribute"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/prioritize"
)

// TestImportComputedPageRankOnCorpus closes the meguri side of the imported-rank
// seam (audit 195) against a real computation rather than hand-set constants. The
// ranks tsumugi imports are a PageRank over the WAT out-link graph, which is the
// offline producer's job; the local corpus carries no link structure, so this test
// stands a faithful in-process producer double in its place: it builds a link graph
// over real corpus URL keys, runs an actual power iteration to convergence, and
// emits the resulting bundle. Everything downstream of the producer is the real
// path: the bundle routes by owning partition over the signal transport, lands on
// the owner's frontier, and moves priority through the blend. The only stand-in is
// the graph itself, which the offline tsumugi job computes over real WAT records.
//
// What this gates that the synthetic-constant test does not: that the import path
// consumes a genuine rank distribution (a converged PageRank, sum one, skew driven
// by in-degree) and that those computed values, not numbers chosen to pass, are
// what route to their owners and raise priority.
func TestImportComputedPageRankOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	disc := loadCorpusDiscoveriesForIntake(t, path)
	if len(disc) < 2000 {
		t.Skipf("corpus produced %d discoveries, need at least 2000", len(disc))
	}

	// Bound the node set so the iteration stays a fast gate; the keys are real and
	// distinct, which is what the import path keys on.
	const n = 6000
	keys := make([]meguri.URLKey, 0, n)
	for _, d := range disc {
		keys = append(keys, d.URLKey)
		if len(keys) == n {
			break
		}
	}

	// hub is node 0: every node links to it, so it carries the most in-degree and
	// must come out the top rank. leaf is the last node: nothing points to it, so it
	// must come out near the floor. The rest of the edges are a deterministic spread
	// that gives the graph real skew without any node going dangling.
	const hub, leaf = 0, n - 1
	out := buildLinkGraph(n, hub, leaf)
	rank := pageRank(out, 0.85, 1e-10, 200)

	// The producer's output must be a real distribution: positive everywhere and
	// summing to one, the property that makes it a PageRank and not an arbitrary
	// weighting.
	var sum float64
	for _, r := range rank {
		if r <= 0 {
			t.Fatalf("pagerank has a non-positive mass %v", r)
		}
		sum += r
	}
	if math.Abs(sum-1) > 1e-6 {
		t.Fatalf("pagerank mass = %v, want 1", sum)
	}
	maxAt := 0
	for i, r := range rank {
		if r > rank[maxAt] {
			maxAt = i
		}
	}
	if maxAt != hub {
		t.Fatalf("highest rank at node %d, want the hub %d", maxAt, hub)
	}
	mean := sum / float64(n)
	if !(rank[leaf] < mean) {
		t.Fatalf("leaf rank %v not below the mean %v, graph has no skew", rank[leaf], mean)
	}

	// Scale the converged distribution into the magnitude tsumugi delivers (average
	// rank near one), then assemble the bundle the producer would emit: a sparse
	// per-URL rank and a dense per-host score aggregated from its pages.
	scale := float64(n)
	hostScore := map[uint64]float64{}
	urls := make([]meguri.URLSignal, n)
	for i, k := range keys {
		r := float32(rank[i] * scale)
		urls[i] = meguri.URLSignal{URLKey: k, PageRank: r}
		hostScore[k.HostKey] += rank[i] * scale
	}
	hosts := make([]meguri.HostSignal, 0, len(hostScore))
	for hk, s := range hostScore {
		hosts = append(hosts, meguri.HostSignal{HostKey: hk, HostScore: float32(s)})
	}
	bundle := meguri.Signal{Epoch: 7, Hosts: hosts, URLs: urls}

	// Route the computed bundle the production way: partition 0 reads the import and
	// calls ImportSignal, which splits by owner, applies its own slice to its
	// frontier, and ships partition 1 its slice over the signal transport.
	mp := &distribute.Map{Epoch: 1, NumPartitions: 2}
	tr := distribute.NewChannelSignalTransport(8)
	sr0 := distribute.NewSignalRouter(0, mp, tr)
	sr1 := distribute.NewSignalRouter(1, mp, tr)
	fr0 := frontier.New(0, 0, frontier.WithPrioritizer(prioritize.DefaultParams()))
	e0 := New(fr0, Config{Fetcher: &recFetcher{clk: NewLogicalClock(0)}, Signals: sr0})
	if err := e0.ImportSignal(bundle); err != nil {
		t.Fatalf("import: %v", err)
	}
	sink1 := newRecSink()
	if n := sr1.Apply(sink1); n == 0 {
		t.Fatal("partition 1 applied no bundle")
	}

	// Every URL partition 1 owns must arrive with the exact computed rank, and no URL
	// partition 0 owns may leak into partition 1's sink.
	var checkedOwned, checkedRemote int
	for i, k := range keys {
		want := urls[i].PageRank
		if mp.Owner(k.HostKey) == 1 {
			got, ok := sink1.urls[k]
			if !ok || got != want {
				t.Fatalf("p1-owned url rank = %v (present %v), want computed %v", got, ok, want)
			}
			checkedOwned++
		} else {
			if _, leaked := sink1.urls[k]; leaked {
				t.Fatalf("p0-owned url %v leaked into partition 1", k)
			}
			checkedRemote++
		}
	}
	if checkedOwned == 0 || checkedRemote == 0 {
		t.Fatalf("split was one-sided: %d owned, %d remote", checkedOwned, checkedRemote)
	}

	// The computed rank must actually move priority, not just be recorded: the hub's
	// converged rank, imported into a fresh prioritizer, raises its blended priority
	// above the no-import baseline.
	pr := prioritize.New(prioritize.DefaultParams())
	rec := &meguri.URLRecord{URLKey: keys[hub]}
	h := &meguri.HostRecord{HostKey: keys[hub].HostKey}
	before := pr.Priority(rec, h)
	pr.ImportPageRank(keys[hub], float32(rank[hub]*scale))
	after := pr.Priority(rec, h)
	if !(after > before) {
		t.Fatalf("computed hub rank did not raise priority: before %v, after %v", before, after)
	}
}

// buildLinkGraph synthesizes the out-link adjacency the offline WAT graph would
// carry, over n nodes. Every node links to the hub, which concentrates in-degree
// there, plus two deterministic spread edges so the rest of the graph has real
// structure; no edge targets the leaf, so it stays the in-degree floor, and no node
// is left dangling. The edge set is fixed by index, so the gate is reproducible.
func buildLinkGraph(n, hub, leaf int) [][]int {
	out := make([][]int, n)
	pick := func(i, salt int) int {
		// A small LCG step keyed by the node and salt, kept off the leaf and off the
		// node itself so an edge never self-loops or feeds the floor node.
		t := (i*1103515245 + salt*12345 + 1013904223) % n
		if t < 0 {
			t += n
		}
		if t == leaf {
			t = hub
		}
		if t == i {
			t = (i + 1) % n
		}
		return t
	}
	for i := range out {
		links := []int{hub}
		if i != hub {
			links = append(links, pick(i, 1), pick(i, 2))
		} else {
			// The hub still needs out-edges so it is not dangling, but it must not
			// link to itself; spread its mass back into the graph.
			links = []int{pick(i, 1), pick(i, 2), 1 % n}
		}
		out[i] = links
	}
	return out
}

// pageRank runs power iteration over an out-link adjacency to convergence, the
// computation the offline producer runs over the WAT graph. It is the standard
// damped formulation: a teleport floor of (1-d)/n, damped contribution along each
// out-edge, and dangling mass (a node with no out-edge) redistributed uniformly. It
// stops when the L1 change falls below tol or after maxIter rounds.
func pageRank(out [][]int, d, tol float64, maxIter int) []float64 {
	n := len(out)
	rank := make([]float64, n)
	for i := range rank {
		rank[i] = 1.0 / float64(n)
	}
	base := (1 - d) / float64(n)
	for range maxIter {
		next := make([]float64, n)
		for i := range next {
			next[i] = base
		}
		var dangling float64
		for i := range out {
			if len(out[i]) == 0 {
				dangling += rank[i]
				continue
			}
			share := d * rank[i] / float64(len(out[i]))
			for _, t := range out[i] {
				next[t] += share
			}
		}
		if dangling > 0 {
			spread := d * dangling / float64(n)
			for i := range next {
				next[i] += spread
			}
		}
		var delta float64
		for i := range next {
			delta += math.Abs(next[i] - rank[i])
		}
		rank = next
		if delta < tol {
			break
		}
	}
	return rank
}
