package format

import m "github.com/tamnd/meguri"

// URL table column ids. The order is the schema; it never changes within a
// format major version. The redundant HostRecord-style HostKey field is not
// stored: it is the high half of urlkey, recovered on decode.
const (
	colURLHostKey      = 0  // u64, high half of URLKey
	colURLPathKey      = 1  // u64, low half of URLKey
	colURLStatus       = 2  // u8
	colURLPriority     = 3  // f32
	colURLDepth        = 4  // u16
	colURLDiscSource   = 5  // u8
	colURLRef          = 6  // u64
	colURLFirstSeen    = 7  // u32
	colURLLastCrawled  = 8  // u32
	colURLLastChanged  = 9  // u32
	colURLNextDue      = 10 // u32
	colURLLambda       = 11 // f32
	colURLCrawlCount   = 12 // u32
	colURLChangeCount  = 13 // u32
	colURLNoChangeStk  = 14 // u16
	colURLETagRef      = 15 // u64
	colURLLastModified = 16 // u32
	colURLContentFP    = 17 // u64
	colURLSimhash      = 18 // u64
	colURLHTTPStatus   = 19 // u16
	colURLRedirectRef  = 20 // u64
	colURLRetryCount   = 21 // u8
	colURLErrorCount   = 22 // u16
	urlColumnCount     = 23
)

// Host table column ids.
const (
	colHostKey          = 0  // u64
	colHostRef          = 1  // u64
	colHostGrouping     = 2  // u8
	colHostRegistRef    = 3  // u64
	colHostResolvedIP   = 4  // [16]byte
	colHostIPExpiry     = 5  // u32
	colHostRobotsFetch  = 6  // u32
	colHostRobotsExpiry = 7  // u32
	colHostRobotsRef    = 8  // u64
	colHostCrawlDelay   = 9  // u16
	colHostNextElig     = 10 // u32
	colHostIPNextElig   = 11 // u32
	colHostURLBudget    = 12 // u32
	colHostURLCount     = 13 // u32
	colHostDepthCap     = 14 // u16
	colHostScore        = 15 // f32
	colHostCrawlTotal   = 16 // u32
	colHostErrorTotal   = 17 // u32
	colHostAvgLatency   = 18 // u16
	colHostFlags        = 19 // u16
	hostColumnCount     = 20
)

// urlColumns flattens the URL records into column-major bytes, one column per
// schema id, all RAW, with the given block codec.
func urlColumns(recs []m.URLRecord, codec uint8) []column {
	n := len(recs)
	hostKey := make([]byte, 0, n*8)
	pathKey := make([]byte, 0, n*8)
	status := make([]byte, 0, n)
	priority := make([]byte, 0, n*4)
	depth := make([]byte, 0, n*2)
	discSrc := make([]byte, 0, n)
	urlRef := make([]byte, 0, n*8)
	firstSeen := make([]byte, 0, n*4)
	lastCrawled := make([]byte, 0, n*4)
	lastChanged := make([]byte, 0, n*4)
	nextDue := make([]byte, 0, n*4)
	lambda := make([]byte, 0, n*4)
	crawlCount := make([]byte, 0, n*4)
	changeCount := make([]byte, 0, n*4)
	noChangeStk := make([]byte, 0, n*2)
	etagRef := make([]byte, 0, n*8)
	lastMod := make([]byte, 0, n*4)
	contentFP := make([]byte, 0, n*8)
	simhash := make([]byte, 0, n*8)
	httpStatus := make([]byte, 0, n*2)
	redirectRef := make([]byte, 0, n*8)
	retryCount := make([]byte, 0, n)
	errorCount := make([]byte, 0, n*2)

	for i := range recs {
		r := &recs[i]
		hostKey = appU64(hostKey, r.URLKey.HostKey)
		pathKey = appU64(pathKey, r.URLKey.PathKey)
		status = appU8(status, uint8(r.Status))
		priority = appF32(priority, r.Priority)
		depth = appU16(depth, r.Depth)
		discSrc = appU8(discSrc, uint8(r.DiscoverySource))
		urlRef = appU64(urlRef, r.URLRef)
		firstSeen = appU32(firstSeen, r.FirstSeen)
		lastCrawled = appU32(lastCrawled, r.LastCrawled)
		lastChanged = appU32(lastChanged, r.LastChanged)
		nextDue = appU32(nextDue, r.NextDue)
		lambda = appF32(lambda, r.Lambda)
		crawlCount = appU32(crawlCount, r.CrawlCount)
		changeCount = appU32(changeCount, r.ChangeCount)
		noChangeStk = appU16(noChangeStk, r.NoChangeStreak)
		etagRef = appU64(etagRef, r.ETagRef)
		lastMod = appU32(lastMod, r.LastModified)
		contentFP = appU64(contentFP, r.ContentFP)
		simhash = appU64(simhash, r.Simhash)
		httpStatus = appU16(httpStatus, r.HTTPStatus)
		redirectRef = appU64(redirectRef, r.RedirectRef)
		retryCount = appU8(retryCount, r.RetryCount)
		errorCount = appU16(errorCount, r.ErrorCount)
	}

	// The nominal encoding per column follows doc 10 section 3: RLE for the
	// constant urlkey_host and the sparse redirect_ref, DELTA_FOR for the
	// ascending urlkey_path, DICTIONARY for the small enums and the quantized
	// floats, DELTA for the clustered timestamps and the monotone url_ref, FOR
	// for the small counters, RAW for the high-entropy fingerprints. The page
	// builder still falls back to RAW per page when the encoding does not beat
	// it, so these are intents, not commitments.
	return []column{
		{colURLHostKey, 8, kindUint, EncRLE, codec, hostKey},
		{colURLPathKey, 8, kindUint, EncDeltaFOR, codec, pathKey},
		{colURLStatus, 1, kindUint, EncDict, codec, status},
		{colURLPriority, 4, kindFloat, EncDict, codec, priority},
		{colURLDepth, 2, kindUint, EncFOR, codec, depth},
		{colURLDiscSource, 1, kindUint, EncDict, codec, discSrc},
		{colURLRef, 8, kindUint, EncDelta, codec, urlRef},
		{colURLFirstSeen, 4, kindUint, EncDelta, codec, firstSeen},
		{colURLLastCrawled, 4, kindUint, EncDelta, codec, lastCrawled},
		{colURLLastChanged, 4, kindUint, EncDelta, codec, lastChanged},
		{colURLNextDue, 4, kindUint, EncDelta, codec, nextDue},
		{colURLLambda, 4, kindFloat, EncDict, codec, lambda},
		{colURLCrawlCount, 4, kindUint, EncFOR, codec, crawlCount},
		{colURLChangeCount, 4, kindUint, EncFOR, codec, changeCount},
		{colURLNoChangeStk, 2, kindUint, EncFOR, codec, noChangeStk},
		{colURLETagRef, 8, kindUint, EncDelta, codec, etagRef},
		{colURLLastModified, 4, kindUint, EncDelta, codec, lastMod},
		{colURLContentFP, 8, kindUint, EncRaw, codec, contentFP},
		{colURLSimhash, 8, kindUint, EncRaw, codec, simhash},
		{colURLHTTPStatus, 2, kindUint, EncDict, codec, httpStatus},
		{colURLRedirectRef, 8, kindUint, EncRLE, codec, redirectRef},
		{colURLRetryCount, 1, kindUint, EncFOR, codec, retryCount},
		{colURLErrorCount, 2, kindUint, EncFOR, codec, errorCount},
	}
}

// urlRecordsFromColumns rebuilds the URL records from decoded column bytes.
func urlRecordsFromColumns(cols map[int][]byte, n int) ([]m.URLRecord, error) {
	for id := range urlColumnCount {
		if _, ok := cols[id]; !ok {
			return nil, ErrCorrupt
		}
	}
	recs := make([]m.URLRecord, n)
	for i := range n {
		r := &recs[i]
		r.URLKey.HostKey = getU64(cols[colURLHostKey], i)
		r.URLKey.PathKey = getU64(cols[colURLPathKey], i)
		r.HostKey = r.URLKey.HostKey
		r.Status = m.URLStatus(getU8(cols[colURLStatus], i))
		r.Priority = getF32(cols[colURLPriority], i)
		r.Depth = getU16(cols[colURLDepth], i)
		r.DiscoverySource = m.DiscoverySource(getU8(cols[colURLDiscSource], i))
		r.URLRef = getU64(cols[colURLRef], i)
		r.FirstSeen = getU32(cols[colURLFirstSeen], i)
		r.LastCrawled = getU32(cols[colURLLastCrawled], i)
		r.LastChanged = getU32(cols[colURLLastChanged], i)
		r.NextDue = getU32(cols[colURLNextDue], i)
		r.Lambda = getF32(cols[colURLLambda], i)
		r.CrawlCount = getU32(cols[colURLCrawlCount], i)
		r.ChangeCount = getU32(cols[colURLChangeCount], i)
		r.NoChangeStreak = getU16(cols[colURLNoChangeStk], i)
		r.ETagRef = getU64(cols[colURLETagRef], i)
		r.LastModified = getU32(cols[colURLLastModified], i)
		r.ContentFP = getU64(cols[colURLContentFP], i)
		r.Simhash = getU64(cols[colURLSimhash], i)
		r.HTTPStatus = getU16(cols[colURLHTTPStatus], i)
		r.RedirectRef = getU64(cols[colURLRedirectRef], i)
		r.RetryCount = getU8(cols[colURLRetryCount], i)
		r.ErrorCount = getU16(cols[colURLErrorCount], i)
	}
	return recs, nil
}

// hostColumns flattens the host records into column-major bytes.
func hostColumns(recs []m.HostRecord, codec uint8) []column {
	n := len(recs)
	hostKey := make([]byte, 0, n*8)
	hostRef := make([]byte, 0, n*8)
	grouping := make([]byte, 0, n)
	registRef := make([]byte, 0, n*8)
	resolvedIP := make([]byte, 0, n*16)
	ipExpiry := make([]byte, 0, n*4)
	robotsFetch := make([]byte, 0, n*4)
	robotsExpiry := make([]byte, 0, n*4)
	robotsRef := make([]byte, 0, n*8)
	crawlDelay := make([]byte, 0, n*2)
	nextElig := make([]byte, 0, n*4)
	ipNextElig := make([]byte, 0, n*4)
	urlBudget := make([]byte, 0, n*4)
	urlCount := make([]byte, 0, n*4)
	depthCap := make([]byte, 0, n*2)
	score := make([]byte, 0, n*4)
	crawlTotal := make([]byte, 0, n*4)
	errorTotal := make([]byte, 0, n*4)
	avgLatency := make([]byte, 0, n*2)
	flags := make([]byte, 0, n*2)

	for i := range recs {
		r := &recs[i]
		hostKey = appU64(hostKey, r.HostKey)
		hostRef = appU64(hostRef, r.HostRef)
		grouping = appU8(grouping, uint8(r.Grouping))
		registRef = appU64(registRef, r.RegistrableRef)
		resolvedIP = append(resolvedIP, r.ResolvedIP[:]...)
		ipExpiry = appU32(ipExpiry, r.IPExpiry)
		robotsFetch = appU32(robotsFetch, r.RobotsFetched)
		robotsExpiry = appU32(robotsExpiry, r.RobotsExpiry)
		robotsRef = appU64(robotsRef, r.RobotsRef)
		crawlDelay = appU16(crawlDelay, r.CrawlDelay)
		nextElig = appU32(nextElig, r.HostNextEligible)
		ipNextElig = appU32(ipNextElig, r.IPNextEligible)
		urlBudget = appU32(urlBudget, r.URLBudget)
		urlCount = appU32(urlCount, r.URLCount)
		depthCap = appU16(depthCap, r.DepthCap)
		score = appF32(score, r.HostScore)
		crawlTotal = appU32(crawlTotal, r.CrawlTotal)
		errorTotal = appU32(errorTotal, r.ErrorTotal)
		avgLatency = appU16(avgLatency, r.AvgLatency)
		flags = appU16(flags, r.Flags)
	}

	// Host columns per doc 10 section 4: DELTA_FOR for the ascending hostkey,
	// DELTA for the offset and timestamp columns, DICTIONARY for the enums and
	// the host score, FOR for the counters, RAW for the 16-byte resolved IP.
	return []column{
		{colHostKey, 8, kindUint, EncDeltaFOR, codec, hostKey},
		{colHostRef, 8, kindUint, EncDelta, codec, hostRef},
		{colHostGrouping, 1, kindUint, EncDict, codec, grouping},
		{colHostRegistRef, 8, kindUint, EncDelta, codec, registRef},
		{colHostResolvedIP, 16, kindRaw, EncRaw, codec, resolvedIP},
		{colHostIPExpiry, 4, kindUint, EncDelta, codec, ipExpiry},
		{colHostRobotsFetch, 4, kindUint, EncDelta, codec, robotsFetch},
		{colHostRobotsExpiry, 4, kindUint, EncDelta, codec, robotsExpiry},
		{colHostRobotsRef, 8, kindUint, EncDelta, codec, robotsRef},
		{colHostCrawlDelay, 2, kindUint, EncDict, codec, crawlDelay},
		{colHostNextElig, 4, kindUint, EncDelta, codec, nextElig},
		{colHostIPNextElig, 4, kindUint, EncDelta, codec, ipNextElig},
		{colHostURLBudget, 4, kindUint, EncFOR, codec, urlBudget},
		{colHostURLCount, 4, kindUint, EncFOR, codec, urlCount},
		{colHostDepthCap, 2, kindUint, EncDict, codec, depthCap},
		{colHostScore, 4, kindFloat, EncDict, codec, score},
		{colHostCrawlTotal, 4, kindUint, EncFOR, codec, crawlTotal},
		{colHostErrorTotal, 4, kindUint, EncFOR, codec, errorTotal},
		{colHostAvgLatency, 2, kindUint, EncFOR, codec, avgLatency},
		{colHostFlags, 2, kindUint, EncDict, codec, flags},
	}
}

// hostRecordsFromColumns rebuilds the host records from decoded column bytes.
func hostRecordsFromColumns(cols map[int][]byte, n int) ([]m.HostRecord, error) {
	for id := range hostColumnCount {
		if _, ok := cols[id]; !ok {
			return nil, ErrCorrupt
		}
	}
	recs := make([]m.HostRecord, n)
	for i := range n {
		r := &recs[i]
		r.HostKey = getU64(cols[colHostKey], i)
		r.HostRef = getU64(cols[colHostRef], i)
		r.Grouping = m.HostGrouping(getU8(cols[colHostGrouping], i))
		r.RegistrableRef = getU64(cols[colHostRegistRef], i)
		copy(r.ResolvedIP[:], cols[colHostResolvedIP][i*16:i*16+16])
		r.IPExpiry = getU32(cols[colHostIPExpiry], i)
		r.RobotsFetched = getU32(cols[colHostRobotsFetch], i)
		r.RobotsExpiry = getU32(cols[colHostRobotsExpiry], i)
		r.RobotsRef = getU64(cols[colHostRobotsRef], i)
		r.CrawlDelay = getU16(cols[colHostCrawlDelay], i)
		r.HostNextEligible = getU32(cols[colHostNextElig], i)
		r.IPNextEligible = getU32(cols[colHostIPNextElig], i)
		r.URLBudget = getU32(cols[colHostURLBudget], i)
		r.URLCount = getU32(cols[colHostURLCount], i)
		r.DepthCap = getU16(cols[colHostDepthCap], i)
		r.HostScore = getF32(cols[colHostScore], i)
		r.CrawlTotal = getU32(cols[colHostCrawlTotal], i)
		r.ErrorTotal = getU32(cols[colHostErrorTotal], i)
		r.AvgLatency = getU16(cols[colHostAvgLatency], i)
		r.Flags = getU16(cols[colHostFlags], i)
	}
	return recs, nil
}
