// Package prioritize decides crawl order by importance (doc 09, the M5
// milestone). It runs the online OPIC computation that spreads cash from crawled
// pages to their out-links, blends in any imported PageRank or host-quality
// signal tsumugi computed over a prior crawl, and produces the Priority the
// frontier's front bank sorts on. It sets the per-host crawl budget from
// cross-host in-degree, the STAR reputation signal an adversary cannot forge,
// and enforces it by deferral, not discard (BEAST). And it carries the spam and
// trap dampeners, the trap-suspect and depth penalties, that keep a link farm
// from monopolizing the schedule.
//
// The package owns policy, not storage. OPIC cash and discounted history are
// resident working state it keeps; the durable result is the URLRecord.Priority
// column the frontier serializes (doc 03). The frontier wires the package in
// opt-in: without it the M1 through M4 dispatch order is unchanged, and with it a
// discovery credits cash and reputation, a crawl distributes cash to its links,
// and every URL's priority is the blended, penalized importance.
package prioritize
