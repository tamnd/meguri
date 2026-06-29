package distribute

import (
	"bufio"
	"encoding/json"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// buildPartition makes a format partition with the given HostKeys, two URL rows
// each, sorted and host-contiguous, with no string arena (the refs are the none
// sentinel). The distribution layer routes on keys and moves whole hosts, so a
// key-only partition is the right fixture here; the string payload is M7's gate.
func buildPartition(hostKeys []uint64) *format.Partition {
	slices.Sort(hostKeys)
	var urls []m.URLRecord
	hosts := make([]m.HostRecord, 0, len(hostKeys))
	for _, hk := range hostKeys {
		hosts = append(hosts, m.HostRecord{HostKey: hk, Grouping: m.GroupFullHost, CrawlDelay: 10, URLCount: 2})
		for p := range uint64(2) {
			urls = append(urls, m.URLRecord{
				URLKey:     m.URLKey{HostKey: hk, PathKey: p + 1},
				HostKey:    hk,
				Status:     m.StatusCrawled,
				HTTPStatus: 200,
			})
		}
	}
	return format.Pack(0, hostKeys[0], hostKeys[len(hostKeys)-1], 480000, format.CodecZstd, urls, hosts, nil)
}

// checkRebalance asserts the invariants every Redistribute must hold: no URL or
// host is lost, every shipped host lands on its new owner, every kept host still
// belongs to self, no host is split across slices, and every slice plus the keep
// encodes and decodes as a well-formed .meguri file.
func checkRebalance(t *testing.T, src *format.Partition, self PartitionID, nm *Map) (shipURLs int) {
	t.Helper()
	ship, keep := Redistribute(src, self, nm)

	seenHost := map[uint64]bool{}
	urlTotal := len(keep.URLs)
	for _, h := range keep.Hosts {
		if nm.Owner(h.HostKey) != self {
			t.Fatalf("kept host %d but its new owner is %d", h.HostKey, nm.Owner(h.HostKey))
		}
		seenHost[h.HostKey] = true
	}
	for dest, slice := range ship {
		urlTotal += len(slice.URLs)
		shipURLs += len(slice.URLs)
		if uint32(dest) != slice.ID {
			t.Fatalf("slice for %d stamped with id %d", dest, slice.ID)
		}
		for _, h := range slice.Hosts {
			if nm.Owner(h.HostKey) != dest {
				t.Fatalf("shipped host %d to %d but its owner is %d", h.HostKey, dest, nm.Owner(h.HostKey))
			}
			if seenHost[h.HostKey] {
				t.Fatalf("host %d appears in more than one slice", h.HostKey)
			}
			seenHost[h.HostKey] = true
		}
		mustRoundTrip(t, slice)
	}
	mustRoundTrip(t, keep)

	if urlTotal != len(src.URLs) {
		t.Fatalf("rebalance lost urls: %d after, %d before", urlTotal, len(src.URLs))
	}
	if len(seenHost) != len(src.Hosts) {
		t.Fatalf("rebalance lost hosts: %d after, %d before", len(seenHost), len(src.Hosts))
	}
	return shipURLs
}

func mustRoundTrip(t *testing.T, p *format.Partition) {
	t.Helper()
	enc, err := format.Encode(p)
	if err != nil {
		t.Fatalf("encode slice: %v", err)
	}
	got, err := format.Decode(enc)
	if err != nil {
		t.Fatalf("decode slice: %v", err)
	}
	if len(got.URLs) != len(p.URLs) || len(got.Hosts) != len(p.Hosts) {
		t.Fatalf("slice counts changed on round trip")
	}
}

// TestRedistributeGrow checks a rebalance onto a freshly added partition: build a
// partition that owns everything under a 1-partition map, grow to 4, and require
// the moving hosts ship to their new owners and the rest stay, with nothing lost.
func TestRedistributeGrow(t *testing.T) {
	var keys []uint64
	for k := range uint64(300) {
		keys = append(keys, splitmix(k))
	}
	src := buildPartition(keys)
	nm := &Map{Epoch: 2, NumPartitions: 4}
	moved := checkRebalance(t, src, 0, nm)
	if moved == 0 {
		t.Fatal("growing to 4 partitions moved no urls off partition 0")
	}
}

// TestMovingHostsMatchesMap checks MovingHosts returns exactly the hosts whose
// owner changed, the set Redistribute slices out.
func TestMovingHostsMatchesMap(t *testing.T) {
	var keys []uint64
	for k := range uint64(200) {
		keys = append(keys, splitmix(k+7))
	}
	src := buildPartition(keys)
	nm := &Map{NumPartitions: 3}
	moving := MovingHosts(src.Hosts, 0, nm)
	want := 0
	for _, hk := range keys {
		if nm.Owner(hk) != 0 {
			want++
		}
	}
	if len(moving) != want {
		t.Fatalf("MovingHosts returned %d, want %d", len(moving), want)
	}
	for _, hk := range moving {
		if nm.Owner(hk) == 0 {
			t.Fatalf("MovingHosts included host %d that partition 0 still owns", hk)
		}
	}
}

// TestRedistributeNoMove checks that when no host's owner changed, nothing ships
// and the keep partition holds the whole source.
func TestRedistributeNoMove(t *testing.T) {
	src := buildPartition([]uint64{1, 2, 3})
	nm := &Map{NumPartitions: 1} // everything still owned by partition 0
	ship, keep := Redistribute(src, 0, nm)
	if len(ship) != 0 {
		t.Fatalf("expected no ship slices, got %d", len(ship))
	}
	if len(keep.URLs) != len(src.URLs) || len(keep.Hosts) != len(src.Hosts) {
		t.Fatal("keep partition lost rows with nothing moving")
	}
}

// TestRebalanceOnCorpus is the M8 gate on real data: load the frozen ccrawl
// slice's real host and URL keys, place them all on partition 0, then grow the
// fleet and rebalance, requiring no URL lost, every host whole on one partition,
// and every shipped slice a well-formed .meguri file. It runs the rebalance over
// real Common Crawl host and URL key distributions, the thing jump hashing's
// balance and minimal movement must hold for at scale.
func TestRebalanceOnCorpus(t *testing.T) {
	path := os.Getenv("MEGURI_CORPUS")
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	src := loadCorpusKeys(t, path)
	if len(src.Hosts) < 4 {
		t.Skipf("corpus has %d hosts, need at least 4 to spread a rebalance", len(src.Hosts))
	}

	for _, n := range []int{4, 16, 64} {
		nm := &Map{Epoch: uint64(n), NumPartitions: n}
		// checkRebalance enforces the invariants that matter on real data: no URL
		// or host lost, every host whole on exactly one partition, and every slice
		// a well-formed .meguri file. The moved URL count is reported, not
		// asserted against a floor, because this corpus has a handful of very
		// uneven hosts, so the moved share is host-placement luck, not a property;
		// the minimal-movement and balance properties are gated on 200k keys in
		// map_test.go where the law of large numbers actually holds.
		moved := checkRebalance(t, src, 0, nm)
		movingHosts := len(MovingHosts(src.Hosts, 0, nm))
		if movingHosts == 0 {
			t.Fatalf("growing to %d partitions moved no hosts off partition 0", n)
		}
		t.Logf("rebalance to %d partitions: %d/%d hosts and %d/%d urls left partition 0, every host whole, every slice round-trips",
			n, movingHosts, len(src.Hosts), moved, len(src.URLs))
	}
}

// loadCorpusKeys reads the corpus into a key-only partition: real URLs and hosts
// derive the 128-bit keys (the routing input), with no string arena, because the
// distribution layer routes on keys and the string payload is M7's gate.
func loadCorpusKeys(tb testing.TB, path string) *format.Partition {
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
	hostOf := map[uint64]bool{}
	urlOf := map[m.URLKey]bool{}
	var urls []m.URLRecord

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
			if _, after, ok := strings.Cut(r.URL, "://"); ok {
				host = after
			} else {
				host = r.URL
			}
			if i := strings.IndexAny(host, "/?#"); i >= 0 {
				host = host[:i]
			}
		}
		if host == "" {
			continue
		}
		path := "/"
		if _, after, ok := strings.Cut(r.URL, "://"); ok {
			if i := strings.IndexAny(after, "/?#"); i >= 0 {
				path = after[i:]
			}
		}
		key := m.MakeURLKey(host, path)
		if urlOf[key] {
			continue
		}
		urlOf[key] = true
		urls = append(urls, m.URLRecord{URLKey: key, HostKey: key.HostKey, Status: m.StatusCrawled, HTTPStatus: 200})
		hostOf[key.HostKey] = true
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("scan corpus: %v", err)
	}

	hosts := make([]m.HostRecord, 0, len(hostOf))
	for hk := range hostOf {
		hosts = append(hosts, m.HostRecord{HostKey: hk, Grouping: m.GroupFullHost, CrawlDelay: 10})
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostKey < hosts[j].HostKey })
	sort.Slice(urls, func(i, j int) bool { return urls[i].URLKey.Less(urls[j].URLKey) })

	lo, hi := uint64(0), ^uint64(0)
	if len(hosts) > 0 {
		lo, hi = hosts[0].HostKey, hosts[len(hosts)-1].HostKey
	}
	return format.Pack(0, lo, hi, 482817, format.CodecZstd, urls, hosts, nil)
}
