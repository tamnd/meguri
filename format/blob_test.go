package format

import (
	"bytes"
	"fmt"
	"testing"
)

// buildFrontCodeArena interns count URL-like strings into a fresh arena in the
// same host-clustered, ascending order the real writer produces, and returns the
// arena bytes along with each interned ref so a test can resolve them back. The
// strings share long prefixes within a host, the sharing front-coding removes.
func buildFrontCodeArena(count int) ([]byte, []uint64, []string) {
	arena := newArena()
	refs := make([]uint64, 0, count)
	strs := make([]string, 0, count)
	for i := range count {
		s := fmt.Sprintf("https://host%03d.example.com/section/%02d/article/%06d", i/500, (i/50)%10, i)
		var off uint64
		arena, off = arenaIntern(arena, []byte(s))
		refs = append(refs, off)
		strs = append(strs, s)
	}
	return arena, refs, strs
}

// TestFrontCodeRoundTrip pins the M1 layout: a front-coded blob region decodes
// back to the exact raw arena bytes (so every *Ref offset resolves identically),
// across both the whole-arena encode and the streaming multi-page encode, at
// chunk sizes that split the arena into many pages. This is the losslessness gate
// front-coding must clear before it can sit under the columns.
func TestFrontCodeRoundTrip(t *testing.T) {
	arena, refs, strs := buildFrontCodeArena(5000)

	// Whole-arena encode/decode.
	region := encodeBlobRegion(arena, CodecZstd, true)
	back, err := decodeBlobRegion(region)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(back, arena) {
		t.Fatalf("front-coded arena round trip differs (%d vs %d bytes)", len(back), len(arena))
	}
	// Every ref still resolves to its original string against the decoded arena.
	for i, off := range refs {
		if got := string(arenaRead(back, off)); got != strs[i] {
			t.Fatalf("ref %d: got %q want %q", i, got, strs[i])
		}
	}

	// Streaming multi-page encode/decode at several chunk sizes, including ones
	// that force many restarts.
	for _, chunk := range []int{64, 1000, 8192, len(arena)} {
		var got bytes.Buffer
		if _, err := streamBlobRegion(&got, bytes.NewReader(arena), int64(len(arena)), chunk, CodecZstd, true); err != nil {
			t.Fatalf("chunk=%d: stream: %v", chunk, err)
		}
		sback, err := decodeBlobRegion(got.Bytes())
		if err != nil {
			t.Fatalf("chunk=%d: decode: %v", chunk, err)
		}
		if !bytes.Equal(sback, arena) {
			t.Fatalf("chunk=%d: streamed arena round trip differs (%d vs %d bytes)", chunk, len(sback), len(arena))
		}
	}
}

// TestFrontCodeSmallerThanRaw is the size lever the bake-off promised: on the
// host-clustered arena, framing the region front-coded produces strictly fewer
// bytes than framing the same arena raw under the same codec. It is a directional
// check, not the 100M number; the measured bytes-per-URL against the 17.60
// baseline is captured on server1 and recorded in the impl doc.
func TestFrontCodeSmallerThanRaw(t *testing.T) {
	arena, _, _ := buildFrontCodeArena(20000)

	raw := encodeBlobRegion(arena, CodecZstd, false)
	fc := encodeBlobRegion(arena, CodecZstd, true)
	if len(fc) >= len(raw) {
		t.Fatalf("front-coding did not shrink the region: raw=%d front-coded=%d", len(raw), len(fc))
	}
	t.Logf("region bytes: raw=%d front-coded=%d (%.1f%% off)", len(raw), len(fc), 100*float64(len(raw)-len(fc))/float64(len(raw)))
}
