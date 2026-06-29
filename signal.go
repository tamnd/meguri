package meguri

// HostSignal is one host's imported reputation: the dense per-HostKey host_score
// tsumugi computes over a prior crawl (doc 09). The blend reads it as the host
// quality term; a host with no signal falls back to its locally accumulated
// score.
type HostSignal struct {
	HostKey   uint64
	HostScore float32
}

// URLSignal is one page's imported PageRank: the sparse per-URLKey rank tsumugi
// delivers for the pages a prior crawl covered (doc 09). A URL with no signal
// scores from its host plus OPIC alone.
type URLSignal struct {
	URLKey   URLKey
	PageRank float32
}

// Signal is one tsumugi import bundle (doc 12, D16): a monotonically rising
// epoch, the dense host scores, and the sparse per-page ranks. It is never
// depended on: a partition that has not yet imported a bundle still crawls
// correctly, and a newer epoch overwrites an older one rather than summing, so a
// dropped or duplicated bundle costs only freshness of the signal, never
// correctness. The bundle a producer emits spans every partition's hosts; the
// router splits it so each partition imports only the entries for hosts it owns.
type Signal struct {
	Epoch uint64       // the import's version, higher supersedes lower
	Hosts []HostSignal // dense host reputation
	URLs  []URLSignal  // sparse per-page rank
}
