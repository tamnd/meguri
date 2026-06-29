package format

import "testing"

// TestGoldenBytes pins the exact serialized form of a fixed partition. The format
// is a durable on-disk contract: a file written by one version must read on
// another, and a change to the byte layout that is not intended is a bug, not a
// refactor. This test encodes a deterministic partition and asserts both the
// byte length and a CRC over the whole file, so any change to the framing,
// column order, encoding choice, or codec output trips here and forces a
// deliberate update of the golden values plus a format-version bump.
//
// When a format change is intentional, run the test, read the failure's actual
// length and CRC, and update the constants below in the same commit as the
// change. Never update them to make a red test green without understanding why
// the bytes moved.
func TestGoldenBytes(t *testing.T) {
	const (
		goldenLen = 5648
		goldenCRC = 0x1db04a1e
	)
	p := buildPartition(t, CodecZstd)
	enc, err := Encode(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got := crc32c(enc)
	if len(enc) != goldenLen || got != goldenCRC {
		t.Fatalf("golden mismatch: len %d crc 0x%08x, want len %d crc 0x%08x\n"+
			"if this change is intentional, update goldenLen/goldenCRC and bump the format version",
			len(enc), got, goldenLen, goldenCRC)
	}
}
