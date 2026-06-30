// Command corpusstat derives the engine-consistent record, URL, host, and
// host-key-range counts over a url-only JSONL corpus, keyed the way the scale and
// bench harness key it: full-host grouping via frontier.HostOf and meguri.HostKeyOf.
// It fills a profile manifest's records, urls, hosts, hostkey_lo, and hostkey_hi
// fields with the engine's view of the corpus, which differs from a naive hostname
// or registrable-domain split. Run it when pinning a new scale profile.
//
//	go run ./cmd/corpusstat corpus/profiles/scale-1m.jsonl
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	meguri "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/frontier"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()

	hosts := map[uint64]struct{}{}
	keys := map[meguri.URLKey]struct{}{}
	var lo, hi uint64
	first := true
	rows := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(line, &rec) != nil || rec.URL == "" {
			continue
		}
		rows++
		host := frontier.HostOf(rec.URL)
		if host == "" {
			continue
		}
		key := meguri.URLKey{
			HostKey: meguri.HostKeyOf(host),
			PathKey: meguri.PathKeyOf(frontier.PathOf(rec.URL)),
		}
		keys[key] = struct{}{}
		hosts[key.HostKey] = struct{}{}
		if first {
			lo, hi, first = key.HostKey, key.HostKey, false
		} else {
			lo = min(lo, key.HostKey)
			hi = max(hi, key.HostKey)
		}
	}
	if err := sc.Err(); err != nil {
		panic(err)
	}
	fmt.Printf("rows=%d\n", rows)
	fmt.Printf("urls=%d\n", len(keys))
	fmt.Printf("hosts=%d\n", len(hosts))
	fmt.Printf("hostkey_lo=0x%016x\n", lo)
	fmt.Printf("hostkey_hi=0x%016x\n", hi)
}
