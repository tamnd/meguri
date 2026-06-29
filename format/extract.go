package format

// Extract splits a partition by host membership: out holds exactly the rows of
// the hosts whose HostKey is in moving, rest holds every other host. Both keep
// the input's sorted order and each gets its own re-based string arena holding
// only the spans its rows reference.
//
// This is the rebalance primitive the distribution layer calls with a jump-hash
// moving set (doc 12, section 3). Unlike Split, which cuts at one HostKey
// boundary, the moving set is a set of non-contiguous HostKeys, because jump
// hashing spreads the hosts that remap onto a new partition across the whole key
// space. Each host's rows are still a contiguous run in the source (the table is
// sorted by URLKey, so by HostKey first), so the selection is a single pass that
// copies whole hosts to one side or the other, never splitting a host.
//
// The two results carry the input's id, creation time, and codec; the caller
// assigns each its destination id and HostKey range as the rebalance needs.
func Extract(p *Partition, moving map[uint64]bool) (out, rest *Partition) {
	out = &Partition{
		ID:           p.ID,
		CreatedHours: p.CreatedHours,
		DefaultCodec: p.DefaultCodec,
		Meta:         cloneMeta(p.Meta),
	}
	rest = &Partition{
		ID:           p.ID,
		CreatedHours: p.CreatedHours,
		DefaultCodec: p.DefaultCodec,
		Meta:         cloneMeta(p.Meta),
	}
	for i := range p.URLs {
		if moving[p.URLs[i].URLKey.HostKey] {
			out.URLs = append(out.URLs, p.URLs[i])
		} else {
			rest.URLs = append(rest.URLs, p.URLs[i])
		}
	}
	for i := range p.Hosts {
		if moving[p.Hosts[i].HostKey] {
			out.Hosts = append(out.Hosts, p.Hosts[i])
		} else {
			rest.Hosts = append(rest.Hosts, p.Hosts[i])
		}
	}
	hostRange(out)
	hostRange(rest)
	rebaseArena(out, p.Strings)
	rebaseArena(rest, p.Strings)
	return out, rest
}

// hostRange sets a partition's HostKeyLo and HostKeyHi to the min and max of its
// host rows, which are sorted, so the ends are the bounds. An empty partition
// keeps the zero range.
func hostRange(p *Partition) {
	if len(p.Hosts) == 0 {
		return
	}
	p.HostKeyLo = p.Hosts[0].HostKey
	p.HostKeyHi = p.Hosts[len(p.Hosts)-1].HostKey
}
