package format

// The seen-set filter region (doc 10 section 6) carries the partition's resident
// approximate dedup filter so a reload does not have to re-add every urlkey to
// rebuild it. The region is one PageFilter page wrapping the opaque filter blob
// the dedup package produced: the format frames it with the block codec and the
// page CRC, but it does not interpret the bytes, so the blocked-Bloom filter
// today and the ribbon static form later ride the same region behind the same
// one-sided membership contract.
//
// The page header's encoding is EncRaw (the blob is already a packed bit array,
// the cascade does not apply) and num_values carries the key count the filter was
// built over, a cheap zone map a reader can show without decoding the body.

// encodeSeensetRegion frames the filter blob as a PageFilter page. keyCount is the
// number of keys the filter holds, stamped into num_values for inspection. An
// empty blob produces no region, the common case for a partition checkpointed
// without a serialized filter.
func encodeSeensetRegion(blob []byte, keyCount uint64, codec uint8) []byte {
	if len(blob) == 0 {
		return nil
	}
	return writePage(PageFilter, EncRaw, codec, uint32(keyCount), 0, 0, blob)
}

// decodeSeensetRegion reads the filter blob back out of a seen-set region,
// verifying the page CRC. It returns the opaque blob the dedup package
// reconstructs the filter from.
func decodeSeensetRegion(region []byte) ([]byte, error) {
	h, payload, _, err := readPage(region)
	if err != nil {
		return nil, err
	}
	if h.kind != PageFilter {
		return nil, ErrCorrupt
	}
	return payload, nil
}
