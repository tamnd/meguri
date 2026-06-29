package engine

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/tamnd/meguri"
	"github.com/tamnd/meguri/distribute"
	"github.com/tamnd/meguri/frontier"
)

// BenchmarkCorpusRoutedIntake measures the receiver half of distribution (doc 12,
// section 6): the rate at which one partition absorbs a routed discovery stream. The
// inbound batches cross the in-process wire transport, so the measured work is the
// real columnar decode plus the idempotent seen-set fold the engine's intake runs,
// not an in-memory slice pass. It is the routed-intake throughput the doc 14
// per-partition table left open, the counterpart to the dispatch-selection rate on
// the send side. It skips when no corpus is configured.
//
// The cost is per discovery and independent of the partition count: a 1-partition or
// a 3334-partition map decodes and folds a received discovery the same way, so the
// number here is the per-partition intake ceiling that holds at fleet scale.
func BenchmarkCorpusRoutedIntake(b *testing.B) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		b.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	disc := loadCorpusDiscoveriesForIntake(b, path)
	if len(disc) < 1000 {
		b.Skipf("corpus produced %d discoveries, need at least 1000", len(disc))
	}

	// Chunk the stream into wire batches the size the router ships, so the receiver
	// drains many messages the way it would on a live fleet.
	const chunk = 4096
	var chunks [][]meguri.Discovery
	for i := 0; i < len(disc); i += chunk {
		chunks = append(chunks, disc[i:min(i+chunk, len(disc))])
	}

	mp := &distribute.Map{Epoch: 1, NumPartitions: 16}
	var total int
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		tr := distribute.NewWireChannelTransport(len(chunks) + 1)
		r := distribute.NewRouter(0, mp, tr, chunk)
		for _, c := range chunks {
			if err := tr.Send(0, c); err != nil { // encode side, off the clock
				b.Fatalf("send: %v", err)
			}
		}
		fr := frontier.New(0, 0)
		b.StartTimer()

		for _, d := range r.Drain() { // decode the wire body
			fr.Discover(d, 0) // fold into the seen-set, the idempotent intake
		}
		total += len(disc)
	}
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "intake/s")
}

// loadCorpusDiscoveriesForIntake reads the corpus URLs into a discovery batch, the
// inbound stream a partition receives after routing. It keys each URL the same way a
// crawled out-link is keyed and synthesizes the per-link fields deterministically by
// index, so the decode and fold run on real keys and real URL strings.
func loadCorpusDiscoveriesForIntake(tb testing.TB, path string) []meguri.Discovery {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	type rec struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	}
	seen := map[meguri.URLKey]bool{}
	var out []meguri.Discovery
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r rec
		if json.Unmarshal([]byte(line), &r) != nil || r.URL == "" {
			continue
		}
		host := r.Host
		if host == "" {
			host = frontier.HostOf(r.URL)
		}
		if host == "" {
			continue
		}
		p := "/"
		if _, after, ok := strings.Cut(r.URL, "://"); ok {
			if i := strings.IndexAny(after, "/?#"); i >= 0 {
				p = after[i:]
			}
		}
		key := meguri.MakeURLKey(host, p)
		if seen[key] {
			continue
		}
		seen[key] = true
		i := len(out)
		out = append(out, meguri.Discovery{
			URLKey:          key,
			CanonicalURL:    r.URL,
			Depth:           uint16(i % 12),
			DiscoverySource: meguri.SourceLink,
			SrcHostKey:      key.HostKey,
			LinkWeight:      float32(i%100) / 100,
			AnchorHint:      meguri.AnchorDescriptive,
			ObservedAt:      uint32(482817 + i%48),
		})
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}
	return out
}
