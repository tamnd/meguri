package format

import (
	"bytes"
	"fmt"
	"testing"
)

// TestArenaSeqReader resolves refs in ascending order over a blob region framed into
// many small pages, so the sliding window spans page boundaries and a span can straddle
// two pages. It is the read path Stage 2 compaction leans on.
func TestArenaSeqReader(t *testing.T) {
	arena := newArena()
	type span struct {
		off  uint64
		want string
	}
	var spans []span
	for i := range 400 {
		s := fmt.Sprintf("https://host%03d.example/path/segment/%d", i, i*7)
		var off uint64
		arena, off = arenaIntern(arena, []byte(s))
		spans = append(spans, span{off, s})
	}

	// Frame the arena into 24-byte pages so most spans cross at least one page edge.
	var region bytes.Buffer
	if _, err := streamBlobRegion(&region, bytes.NewReader(arena), int64(len(arena)), 24, CodecZstd); err != nil {
		t.Fatalf("streamBlobRegion: %v", err)
	}

	// The whole-arena decode must reproduce the input, the invariant the sequential
	// reader shares its offsets with.
	got, err := decodeBlobRegion(region.Bytes())
	if err != nil {
		t.Fatalf("decodeBlobRegion: %v", err)
	}
	if !bytes.Equal(got, arena) {
		t.Fatalf("decodeBlobRegion round-trip differs: %d vs %d bytes", len(got), len(arena))
	}

	r := &ArenaSeqReader{region: region.Bytes()}
	if s, err := r.At(0); err != nil || s != nil {
		t.Fatalf("zero ref: got %q err %v, want nil", s, err)
	}
	for _, sp := range spans {
		s, err := r.At(sp.off)
		if err != nil {
			t.Fatalf("At(%d): %v", sp.off, err)
		}
		if string(s) != sp.want {
			t.Fatalf("At(%d) = %q, want %q", sp.off, s, sp.want)
		}
	}

	// A backward ref breaks the contract and must be reported, not silently misread.
	if _, err := r.At(spans[0].off); err != ErrArenaBackward {
		t.Fatalf("backward ref: err = %v, want ErrArenaBackward", err)
	}
}

// TestArenaSeqReaderEmpty covers a file with no blob region: every ref resolves nil.
func TestArenaSeqReaderEmpty(t *testing.T) {
	r := &ArenaSeqReader{full: true}
	if s, err := r.At(0); err != nil || s != nil {
		t.Fatalf("empty zero: %q %v", s, err)
	}
	if s, err := r.At(5); err != nil || s != nil {
		t.Fatalf("empty ref: %q %v", s, err)
	}
}
