// Package prioritize decides crawl order by importance: it runs the online OPIC
// computation that spreads cash from crawled pages to their out-links, folds in
// any imported PageRank or host-quality signal, and writes the Priority field of
// meguri.URLRecord the frontier's front banks sort on. It also carries the spam
// and trap dampeners that keep a link farm from monopolizing the schedule.
//
// This is the M5 milestone. The package is a placeholder until then.
package prioritize
