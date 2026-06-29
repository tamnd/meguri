package store

import (
	"bytes"
	"testing"
)

// TestInternRobotsRoundTrip checks a robots blob interned through the packing
// modes reads back byte for byte, and that an allow-all (empty) blob takes the
// none sentinel rather than growing the arena.
func TestInternRobotsRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir(), Options{Durability: DurabilityNone})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	allowAll, err := s.InternRobots(nil)
	if err != nil {
		t.Fatal(err)
	}
	if allowAll != 0 {
		t.Fatalf("allow-all robots interned to ref %d, want 0 (none sentinel)", allowAll)
	}
	if got := s.Robots(0); got != nil {
		t.Fatalf("allow-all read back %q, want nil", got)
	}

	blob := bytes.Repeat([]byte("User-agent: *\nDisallow: /private\n"), 50)
	ref, err := s.InternRobots(blob)
	if err != nil {
		t.Fatal(err)
	}
	if ref == 0 {
		t.Fatalf("non-empty robots interned to the none sentinel")
	}
	if got := s.Robots(ref); !bytes.Equal(got, blob) {
		t.Fatalf("robots round-trip mismatch: %d bytes in, %d out", len(blob), len(got))
	}
	// A stale or out-of-range ref degrades to nil (allow-all) rather than panicking.
	if got := s.Robots(ref + 1<<40); got != nil {
		t.Fatalf("out-of-range ref read %q, want nil", got)
	}
}

// TestInternRobotsSurvivesReplay checks a robots ref and its blob survive a log
// replay: the packed entry is logged exactly like a string intern, so recovery
// rebuilds the arena to the same offset and the blob unpacks to the same bytes.
func TestInternRobotsSurvivesReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Durability: DurabilityFull})
	if err != nil {
		t.Fatal(err)
	}
	blob := bytes.Repeat([]byte("Disallow: /tmp\n"), 40)
	ref, err := s.InternRobots(blob)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(dir, Options{Durability: DurabilityFull})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := r.Robots(ref); !bytes.Equal(got, blob) {
		t.Fatalf("replay lost the robots blob at ref %d: %d bytes recovered", ref, len(got))
	}
}
