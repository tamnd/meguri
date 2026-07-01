package format

import (
	"reflect"
	"strconv"
	"testing"
)

// roundTripEncoding runs one column's values through encode then decode at the
// given width and asserts they come back identical. It exercises the same path
// the page builder uses: encodeValues to a payload plus base, decodeValues back.
func roundTripEncoding(t *testing.T, enc uint8, vals []uint64, width int) {
	t.Helper()
	payload, base := encodeValues(enc, vals, width)
	got, err := decodeValues(enc, payload, len(vals), width, base)
	if err != nil {
		t.Fatalf("enc %d width %d: decode: %v", enc, width, err)
	}
	// Mask the expected values to the width, since encode/decode operate on the
	// width's range and a caller never reads back more than it wrote.
	want := make([]uint64, len(vals))
	for i, v := range vals {
		want[i] = maskWidth(v, width)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enc %d width %d: round trip mismatch\n want %v\n got  %v", enc, width, want, got)
	}
}

func maskWidth(v uint64, width int) uint64 {
	switch width {
	case 1:
		return v & 0xff
	case 2:
		return v & 0xffff
	case 4:
		return v & 0xffffffff
	default:
		return v
	}
}

func TestEncodingRoundTrips(t *testing.T) {
	cases := []struct {
		name  string
		enc   uint8
		width int
		vals  []uint64
	}{
		{"rle-constant", EncRLE, 8, []uint64{7, 7, 7, 7, 7}},
		{"rle-runs", EncRLE, 8, []uint64{1, 1, 2, 2, 2, 9}},
		{"rle-sparse", EncRLE, 8, []uint64{0, 0, 0, 5, 0, 0}},
		{"rle-single", EncRLE, 4, []uint64{42}},
		{"rle-empty", EncRLE, 4, []uint64{}},
		{"dict-enum", EncDict, 1, []uint64{1, 5, 1, 5, 5, 1}},
		{"dict-one", EncDict, 2, []uint64{3, 3, 3}},
		{"dict-wide", EncDict, 4, []uint64{100, 200, 300, 100, 200}},
		{"dict-empty", EncDict, 1, []uint64{}},
		{"delta-monotone", EncDelta, 8, []uint64{0, 12, 24, 36}},
		{"delta-clustered", EncDelta, 4, []uint64{470100, 470050, 470080, 470060}},
		{"delta-flat", EncDelta, 4, []uint64{500, 500, 500}},
		{"delta-single", EncDelta, 8, []uint64{99}},
		{"delta-empty", EncDelta, 8, []uint64{}},
		{"for-narrow", EncFOR, 4, []uint64{100, 101, 100, 102, 100}},
		{"for-constant", EncFOR, 2, []uint64{5, 5, 5}},
		{"for-wide", EncFOR, 8, []uint64{0, 1 << 40, 7}},
		{"for-empty", EncFOR, 4, []uint64{}},
		{"deltafor-ascending", EncDeltaFOR, 8, []uint64{0x00AA, 0x00BB, 0x00CC, 0x0100}},
		{"deltafor-hostkeys", EncDeltaFOR, 8, []uint64{0x1111_1111, 0x2222_2222, 0x3333_3333}},
		{"deltafor-single", EncDeltaFOR, 8, []uint64{0x55}},
		{"deltafor-two", EncDeltaFOR, 8, []uint64{10, 17}},
		{"deltafor-empty", EncDeltaFOR, 8, []uint64{}},
		{"raw-passthrough", EncRaw, 4, []uint64{1, 2, 3, 0xdeadbeef}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			roundTripEncoding(t, c.enc, c.vals, c.width)
		})
	}
}

func TestBitpackRoundTrip(t *testing.T) {
	for _, width := range []uint8{0, 1, 3, 7, 12, 17, 33, 64} {
		vals := make([]uint64, 50)
		var mask uint64 = 1<<width - 1
		if width == 64 {
			mask = ^uint64(0)
		}
		for i := range vals {
			vals[i] = uint64(i*2654435761) & mask
		}
		packed := bitpack(vals, width)
		got := bitunpack(packed, len(vals), width)
		if !reflect.DeepEqual(got, vals) {
			t.Fatalf("width %d: bitpack round trip mismatch", width)
		}
	}
}

// bitunpackScalar is the pre-M6 bit-at-a-time reference kept in the test so the
// word-at-a-time kernel can be checked bit-identical against it across widths.
func bitunpackScalar(b []byte, n int, width uint8) []uint64 {
	out := make([]uint64, n)
	if width == 0 {
		return out
	}
	bitpos := 0
	for i := range out {
		var v uint64
		for k := uint8(0); k < width; k++ {
			if bitpos>>3 < len(b) && b[bitpos>>3]&(1<<uint(bitpos&7)) != 0 {
				v |= 1 << k
			}
			bitpos++
		}
		out[i] = v
	}
	return out
}

// TestBitunpackMatchesScalar is the M6 correctness gate: the word-at-a-time kernel
// must return byte-for-byte what the old bit-at-a-time loop did, for every width a
// column can carry (1..64) and across counts that straddle 8-byte boundaries and the
// packed tail, including a value whose field spills past the 64-bit load window.
func TestBitunpackMatchesScalar(t *testing.T) {
	for width := uint8(1); width <= 64; width++ {
		var mask uint64 = 1<<width - 1
		if width == 64 {
			mask = ^uint64(0)
		}
		for _, n := range []int{1, 2, 7, 8, 9, 63, 64, 65, 127, 200} {
			vals := make([]uint64, n)
			for i := range vals {
				vals[i] = (uint64(i)*0x9E3779B97F4A7C15 + uint64(width)) & mask
			}
			packed := bitpack(vals, width)
			want := bitunpackScalar(packed, n, width)
			got := bitunpack(packed, n, width)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("width %d n %d: kernel disagrees with scalar\n want %v\n got  %v", width, n, want, got)
			}
			// And it must reconstruct the values it packed.
			if !reflect.DeepEqual(got, vals) {
				t.Fatalf("width %d n %d: kernel did not round-trip the packed values", width, n)
			}
		}
	}
}

// BenchmarkBitunpack measures the kernel across the widths a real store carries, so
// the M6 speedup is a captured number and not a claim.
func BenchmarkBitunpack(b *testing.B) {
	for _, width := range []uint8{4, 8, 12, 16, 24, 32, 48, 64} {
		var mask uint64 = 1<<width - 1
		if width == 64 {
			mask = ^uint64(0)
		}
		const n = 4096
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = (uint64(i)*0x9E3779B97F4A7C15 + uint64(width)) & mask
		}
		packed := bitpack(vals, width)
		b.Run("w"+strconv.Itoa(int(width)), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for range b.N {
				sink := bitunpack(packed, n, width)
				_ = sink
			}
		})
		b.Run("scalar-w"+strconv.Itoa(int(width)), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for range b.N {
				sink := bitunpackScalar(packed, n, width)
				_ = sink
			}
		})
	}
}

func TestZigzagRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 2, -2, 1 << 40, -(1 << 40), 1<<62 - 1} {
		if got := unzigzag(zigzag(v)); got != v {
			t.Fatalf("zigzag round trip: %d -> %d", v, got)
		}
	}
}

func TestBitWidth(t *testing.T) {
	cases := map[uint64]uint8{0: 0, 1: 1, 2: 2, 3: 2, 7: 3, 8: 4, 255: 8, 256: 9}
	for v, want := range cases {
		if got := bitWidth(v); got != want {
			t.Fatalf("bitWidth(%d) = %d, want %d", v, got, want)
		}
	}
}
