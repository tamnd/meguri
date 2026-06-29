package distribute

import (
	"sort"

	m "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/format"
)

// TailKind tags a replication tail entry, the decoded form of one store log
// frame (doc 11): a record write or a tombstone. The store ships its snapshot
// once and then streams these in LSN order; a replica applies them exactly as
// recovery replays the log, so a replica is a partition recovered up to the tail
// it has received (doc 12, section 4).
type TailKind uint8

const (
	TailPutURL  TailKind = iota // a URLRecord write
	TailPutHost                 // a HostRecord write
	TailDelURL                  // a URL tombstone (a key left the partition)
	TailDelHost                 // a host tombstone
)

// TailEntry is one entry of the replicated log tail, carrying the LSN that
// orders it. The LSN is the store's monotonic per-partition sequence number, so
// a replica applies entries in LSN order and ignores any it already holds, which
// makes the stream idempotent and safe to redeliver, the same redo-only,
// last-writer-wins discipline the store's own recovery uses (doc 11, section 5).
type TailEntry struct {
	LSN     uint64
	Kind    TailKind
	URL     m.URLRecord // set for TailPutURL
	Host    m.HostRecord
	URLKey  m.URLKey // set for TailDelURL
	HostKey uint64   // set for TailDelHost
}

// PutURL builds a tail entry for a URL write at the given LSN.
func PutURL(lsn uint64, rec m.URLRecord) TailEntry {
	return TailEntry{LSN: lsn, Kind: TailPutURL, URL: rec}
}

// PutHost builds a tail entry for a host write at the given LSN.
func PutHost(lsn uint64, rec m.HostRecord) TailEntry {
	return TailEntry{LSN: lsn, Kind: TailPutHost, Host: rec}
}

// DelURL builds a tail entry for a URL tombstone at the given LSN.
func DelURL(lsn uint64, key m.URLKey) TailEntry {
	return TailEntry{LSN: lsn, Kind: TailDelURL, URLKey: key}
}

// DelHost builds a tail entry for a host tombstone at the given LSN.
func DelHost(lsn uint64, hostKey uint64) TailEntry {
	return TailEntry{LSN: lsn, Kind: TailDelHost, HostKey: hostKey}
}

// Replica is a partition's standby copy: a snapshot loaded once, then the log
// tail streamed onto it in LSN order. At every moment it is a partition that has
// recovered up to the last tail entry it applied, so promotion (section 5) is
// just materializing it and resetting in-flight work, with no new mechanism: the
// snapshot ship is the rebalance file ship and the tail stream is the recovery
// replay (doc 12, section 4).
type Replica struct {
	id      uint32
	created uint32
	codec   uint8

	urls    map[m.URLKey]m.URLRecord
	hosts   map[uint64]m.HostRecord
	strings []byte

	applied uint64 // the highest LSN this replica has applied
	base    uint64 // the snapshot's frontier LSN, the floor for the tail
}

// NewReplica loads a shipped snapshot as the replica's base state. snapLSN is the
// frontier LSN the snapshot was consistent as of (the store's Checkpoint cut, doc
// 11), so the replica only applies tail entries past it.
func NewReplica(snap *format.Partition, snapLSN uint64) *Replica {
	r := &Replica{
		id:      snap.ID,
		created: snap.CreatedHours,
		codec:   snap.DefaultCodec,
		urls:    make(map[m.URLKey]m.URLRecord, len(snap.URLs)),
		hosts:   make(map[uint64]m.HostRecord, len(snap.Hosts)),
		strings: append([]byte(nil), snap.Strings...),
		applied: snapLSN,
		base:    snapLSN,
	}
	for _, u := range snap.URLs {
		r.urls[u.URLKey] = u
	}
	for _, h := range snap.Hosts {
		r.hosts[h.HostKey] = h
	}
	return r
}

// Apply folds one tail entry into the replica. An entry at or below the applied
// LSN is a redelivery the replica already holds, so it is ignored; this is what
// lets the replication transport be at-least-once with no commit protocol, the
// same economy the discovery transport relies on (doc 12, sections 4 and 6).
func (r *Replica) Apply(e TailEntry) {
	if e.LSN <= r.applied {
		return
	}
	switch e.Kind {
	case TailPutURL:
		r.urls[e.URL.URLKey] = e.URL
	case TailPutHost:
		r.hosts[e.Host.HostKey] = e.Host
	case TailDelURL:
		delete(r.urls, e.URLKey)
	case TailDelHost:
		delete(r.hosts, e.HostKey)
	}
	r.applied = e.LSN
}

// Stream applies a batch of tail entries in LSN order. The caller may hand them
// in any order; Replica sorts by LSN so an out-of-order or batched delivery
// still lands last-writer-wins.
func (r *Replica) Stream(entries []TailEntry) {
	ordered := append([]TailEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].LSN < ordered[j].LSN })
	for _, e := range ordered {
		r.Apply(e)
	}
}

// AppliedLSN is the highest LSN the replica has folded in, the point its state
// is recovered to.
func (r *Replica) AppliedLSN() uint64 { return r.applied }

// Lag is how far behind the primary the replica is: the LSNs the primary has
// written but not yet streamed. It is the bounded staleness section 4 names, the
// few writes a promoted replica re-crawls, which cost a redundant polite fetch
// rather than a lost URL.
func (r *Replica) Lag(primaryLSN uint64) uint64 {
	if primaryLSN <= r.applied {
		return 0
	}
	return primaryLSN - r.applied
}

// Partition materializes the replica's current state as a sorted format.Partition,
// the same shape a checkpoint produces, so a promoted replica writes a normal
// .meguri file indistinguishable from one the failed primary would have written.
func (r *Replica) Partition() *format.Partition {
	urls := make([]m.URLRecord, 0, len(r.urls))
	for _, u := range r.urls {
		urls = append(urls, u)
	}
	sort.Slice(urls, func(i, j int) bool { return urls[i].URLKey.Less(urls[j].URLKey) })

	hosts := make([]m.HostRecord, 0, len(r.hosts))
	for _, h := range r.hosts {
		hosts = append(hosts, h)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostKey < hosts[j].HostKey })

	lo, hi := uint64(0), ^uint64(0)
	if len(hosts) > 0 {
		lo, hi = hosts[0].HostKey, hosts[len(hosts)-1].HostKey
	}
	return &format.Partition{
		ID:           r.id,
		HostKeyLo:    lo,
		HostKeyHi:    hi,
		CreatedHours: r.created,
		DefaultCodec: r.codec,
		URLs:         urls,
		Hosts:        hosts,
		Strings:      append([]byte(nil), r.strings...),
	}
}

// Promote turns the replica into the partition it will run as: it materializes
// the recovered state and resets in-flight work conservatively (section 5). The
// returned partition is ready to load into a live store and resume crawling.
func (r *Replica) Promote() *format.Partition {
	p := r.Partition()
	RecoverInFlight(p)
	return p
}

// RecoverInFlight resets every URL stuck InFlight back to Scheduled and returns
// how many it reset. A URL InFlight at a failure either was never fetched or its
// outcome was lost, and re-fetching is the safe choice: a redundant polite fetch
// wastes one request, a lost fetch loses a crawl, so recovery always errs toward
// the redundant fetch (doc 12, section 5; doc 04, the recovery section).
func RecoverInFlight(p *format.Partition) int {
	reset := 0
	for i := range p.URLs {
		if p.URLs[i].Status == m.StatusInFlight {
			p.URLs[i].Status = m.StatusScheduled
			reset++
		}
	}
	return reset
}
