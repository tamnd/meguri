// Command strmeasure is the Spec 2074 M1 string-blob bake-off. It reads a real
// URL sample (one per line) and reports the bytes per URL each candidate blob
// layout reaches, so the file change is chosen on measured bytes and not on a
// citation. It is a measurement tool, not part of the engine.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/tamnd/meguri/format"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: strmeasure <urls.txt>")
		os.Exit(2)
	}
	urls := readLines(os.Args[1])
	n := len(urls)
	var raw int
	for _, u := range urls {
		raw += len(u)
	}
	fmt.Printf("urls=%d  raw=%.2f B/url  (%d bytes)\n", n, float64(raw)/float64(n), raw)

	// The sample arrives lexically sorted (the corpus is). Measure that order,
	// which is the order an arena sorted by URL string would use, and also a
	// host-clustered order (group by host, lexical within) which is closer to
	// today's hostkey-clustered arena but keeps lexical adjacency inside a host.
	measure("lexical", urls, n)

	hc := make([]string, len(urls))
	copy(hc, urls)
	sort.SliceStable(hc, func(i, j int) bool {
		hi, hj := host(hc[i]), host(hc[j])
		if hi != hj {
			return hi < hj
		}
		return hc[i] < hc[j]
	})
	measure("host-clustered", hc, n)

	// The ordering probe: the real arena clusters by hostkey (a hash), so hosts
	// land in scattered order and paths inside a host in hash order too. Bracket
	// that with a full shuffle (worst case, no adjacency) and a host-grouped shuffle
	// (hosts contiguous but scattered order, paths shuffled inside), reporting only
	// raw+zstd since that is the layout the file uses and the one order moves.
	fmt.Printf("\n== ordering probe (A raw+zstd only) ==\n")
	fmt.Printf("lexical            %6.2f B/url\n", perURL(zstdLen(concat(urls)), n))

	sh := shuffleCopy(urls, 1)
	fmt.Printf("shuffled           %6.2f B/url\n", perURL(zstdLen(concat(sh)), n))

	hg := hostGroupShuffle(urls, 2)
	fmt.Printf("host-group-shuffle %6.2f B/url\n", perURL(zstdLen(concat(hg)), n))
}

// shuffleCopy returns a deterministically shuffled copy.
func shuffleCopy(urls []string, seed int64) []string {
	out := make([]string, len(urls))
	copy(out, urls)
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// hostGroupShuffle keeps each host's URLs contiguous but scrambles both the order
// of hosts and the order of URLs inside a host, mimicking a hostkey-hash arena.
func hostGroupShuffle(urls []string, seed int64) []string {
	byHost := map[string][]string{}
	var order []string
	for _, u := range urls {
		h := host(u)
		if _, ok := byHost[h]; !ok {
			order = append(order, h)
		}
		byHost[h] = append(byHost[h], u)
	}
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	out := make([]string, 0, len(urls))
	for _, h := range order {
		g := byHost[h]
		r.Shuffle(len(g), func(i, j int) { g[i], g[j] = g[j], g[i] })
		out = append(out, g...)
	}
	return out
}

func measure(order string, urls []string, n int) {
	fmt.Printf("\n== order: %s ==\n", order)

	// A: raw arena, zstd. The current layout.
	arena := concat(urls)
	fmt.Printf("A raw+zstd        %6.2f B/url\n", perURL(zstdLen(arena), n))

	// B: FSST spans, no zstd. Per-ref random access, ratio is FSST's alone.
	strs := toBytes(urls)
	fa, _ := format.BuildFSSTArena(strs)
	tbl, spans := fa.Bytes()
	fmt.Printf("B fsst spans      %6.2f B/url  (table=%d)\n", perURL(len(tbl)+len(spans), n), len(tbl))

	// C: FSST spans, then zstd the span region. Same whole-page access as A.
	fmt.Printf("C fsst+zstd       %6.2f B/url\n", perURL(len(tbl)+zstdLen(spans), n))

	// D: front-coded (LCP + suffix), then zstd.
	fc := frontCode(urls)
	fmt.Printf("D frontcode+zstd  %6.2f B/url\n", perURL(zstdLen(fc), n))

	// E: front-coded suffixes, FSST, then zstd the spans, plus the zstd'd LCP stream.
	lcps, suffixes := frontParts(urls)
	fe, _ := format.BuildFSSTArena(suffixes)
	etbl, espans := fe.Bytes()
	lcpBytes := uvarints(lcps)
	total := len(etbl) + zstdLen(espans) + zstdLen(lcpBytes)
	fmt.Printf("E fc+fsst+zstd    %6.2f B/url\n", perURL(total, n))
}

// concat joins strings into one arena, each terminated so refs can span it.
func concat(urls []string) []byte {
	var b []byte
	for _, u := range urls {
		b = append(b, u...)
		b = append(b, '\n')
	}
	return b
}

// frontCode emits, per string, the shared-prefix length with the previous string
// as a uvarint, then the suffix length as a uvarint, then the suffix bytes.
func frontCode(urls []string) []byte {
	var out []byte
	var prev string
	for _, u := range urls {
		l := commonPrefix(prev, u)
		out = binary.AppendUvarint(out, uint64(l))
		out = binary.AppendUvarint(out, uint64(len(u)-l))
		out = append(out, u[l:]...)
		prev = u
	}
	return out
}

// frontParts splits front-coding into the LCP array and the suffix byte slices,
// so the suffixes can be FSST-trained on their own.
func frontParts(urls []string) ([]uint64, [][]byte) {
	lcps := make([]uint64, len(urls))
	suffixes := make([][]byte, len(urls))
	var prev string
	for i, u := range urls {
		l := commonPrefix(prev, u)
		lcps[i] = uint64(l)
		suffixes[i] = []byte(u[l:])
		prev = u
	}
	return lcps, suffixes
}

func commonPrefix(a, b string) int {
	m := min(len(a), len(b))
	i := 0
	for i < m && a[i] == b[i] {
		i++
	}
	return i
}

func uvarints(vs []uint64) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.AppendUvarint(b, v)
	}
	return b
}

func toBytes(urls []string) [][]byte {
	b := make([][]byte, len(urls))
	for i, u := range urls {
		b[i] = []byte(u)
	}
	return b
}

func host(u string) string {
	i := 0
	if j := indexOf(u, "://"); j >= 0 {
		i = j + 3
	}
	rest := u[i:]
	if s := indexByte(rest, '/'); s >= 0 {
		return rest[:s]
	}
	return rest
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

var enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))

func zstdLen(b []byte) int { return len(enc.EncodeAll(b, nil)) }

func perURL(bytes, n int) float64 { return float64(bytes) / float64(n) }

func readLines(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return out
}
