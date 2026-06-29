package format

import (
	"bytes"
	"testing"

	m "github.com/tamnd/meguri"
)

// TestDictReclaimsOnCorpus drives the content dictionary with the real corpus
// spans under the duplication a fleet produces: two partitions that have each
// discovered the same hosts and URLs, so the same string content is interned
// twice. It feeds every span the corpus references through one dictionary twice
// and checks the second pass adds nothing, so the deduped arena holds one copy
// where a naive append would hold two. It skips when no corpus is configured.
func TestDictReclaimsOnCorpus(t *testing.T) {
	path := corpusPath()
	if path == "" {
		t.Skip("set MEGURI_CORPUS to a ccrawl jsonl slice (see scripts/fetch-corpus.sh)")
	}
	p := loadCorpus(t, path)
	if len(p.URLs) < 1000 {
		t.Skipf("corpus has %d urls, need at least 1000", len(p.URLs))
	}

	// The spans the corpus actually references: every URL string plus each host's
	// host and registrable spans, read back out of the loaded arena.
	var spans [][]byte
	var naive int
	for _, u := range p.URLs {
		s := arenaRead(p.Strings, u.URLRef)
		spans = append(spans, s)
		naive += len(s)
	}
	for _, h := range p.Hosts {
		hs := arenaRead(p.Strings, h.HostRef)
		rs := arenaRead(p.Strings, h.RegistrableRef)
		spans = append(spans, hs, rs)
		naive += len(hs) + len(rs)
	}

	// Intern every span, then intern every span a second time, the second
	// partition's worth of the same content. A dictionary folds the repeats, so its
	// arena after both passes equals its arena after the first.
	d := newDict()
	for _, s := range spans {
		d.intern(s)
	}
	afterOne := len(d.arena)
	for _, s := range spans {
		d.intern(s)
	}
	afterTwo := len(d.arena)
	if afterTwo != afterOne {
		t.Fatalf("second pass over identical content grew the arena %d -> %d, dictionary did not fold the repeats", afterOne, afterTwo)
	}

	// Every distinct span still resolves through its interned offset.
	for i := 0; i < len(spans); i += len(spans)/101 + 1 {
		off := d.by[string(spans[i])]
		if string(arenaRead(d.arena, off)) != string(spans[i]) {
			t.Fatalf("span %d did not resolve through the dictionary", i)
		}
	}
	t.Logf("dict on corpus: %d spans (%d distinct), two passes hold one copy at %d bytes, naive two-copy append would be ~%d",
		len(spans)*2, len(d.by), afterTwo, naive*2)
}

// TestDictInternsDistinctOnce checks the content dictionary returns one offset
// for equal spans and distinct offsets for distinct spans, the property that
// folds a repeated host or registrable string down to a single copy.
func TestDictInternsDistinctOnce(t *testing.T) {
	d := newDict()
	a := d.intern([]byte("example.com"))
	b := d.intern([]byte("example.com"))
	c := d.intern([]byte("other.org"))
	if a != b {
		t.Fatalf("equal spans interned to %d and %d, want one offset", a, b)
	}
	if a == c {
		t.Fatalf("distinct spans shared offset %d", a)
	}
	if got := string(arenaRead(d.arena, a)); got != "example.com" {
		t.Fatalf("read back %q, want example.com", got)
	}
	if got := string(arenaRead(d.arena, c)); got != "other.org" {
		t.Fatalf("read back %q, want other.org", got)
	}
}

// TestMergeDedupsSharedSpans checks a merge folds a registrable domain both
// partitions carry down to one arena copy. Each partition has two hosts under
// example.com, so the merged arena holds the registrable span once, not four
// times.
func TestMergeDedupsSharedSpans(t *testing.T) {
	build := func(idBase uint64) *Partition {
		arena := newArena()
		var regOff uint64
		arena, regOff = arenaIntern(arena, []byte("example.com"))
		var h1, h2 uint64
		arena, h1 = arenaIntern(arena, []byte("a.example.com"))
		arena, h2 = arenaIntern(arena, []byte("b.example.com"))
		hosts := []m.HostRecord{
			{HostKey: idBase, HostRef: h1, RegistrableRef: regOff},
			{HostKey: idBase + 1, HostRef: h2, RegistrableRef: regOff},
		}
		return &Partition{
			HostKeyLo: idBase, HostKeyHi: idBase + 1,
			Hosts: hosts, Strings: arena,
		}
	}
	lo := build(10)
	hi := build(100)

	merged, err := Merge(lo, hi)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	// The registrable string appears once in the merged arena even though four
	// hosts across the two partitions reference it.
	if n := bytes.Count(merged.Strings, []byte("example.com")); n != 3 {
		// "example.com" is a substring of "a.example.com" and "b.example.com", so the
		// distinct host spans (two of them, each partition's a/b dedup to one pair)
		// contribute two and the standalone registrable contributes one.
		t.Fatalf("merged arena holds %d copies of the registrable text, want 3 (two host spans plus one shared registrable)", n)
	}
	// Every host still resolves to the right registrable string through its ref.
	for _, h := range merged.Hosts {
		if got := string(arenaRead(merged.Strings, h.RegistrableRef)); got != "example.com" {
			t.Fatalf("host %d registrable resolved to %q", h.HostKey, got)
		}
	}
}

// TestPackRobotsModes checks each of the three packing modes round-trips and that
// the packer picks the smallest form: the allow-all sentinel for an empty blob,
// compressed for a long repetitive blob, raw for a short incompressible one.
func TestPackRobotsModes(t *testing.T) {
	cases := []struct {
		name     string
		blob     []byte
		wantMode byte
	}{
		{"allow-all", nil, robotsAllowAll},
		{"short raw", []byte("User-agent: *\nDisallow: /x"), robotsRaw},
		{"compressible", bytes.Repeat([]byte("Disallow: /a\n"), 200), robotsCompressed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			packed := PackRobots(c.blob, CodecZstd)
			if packed[0] != c.wantMode {
				t.Fatalf("mode = %d, want %d", packed[0], c.wantMode)
			}
			got, ok := UnpackRobots(packed, CodecZstd, RobotsSizeHint)
			if !ok {
				t.Fatalf("unpack reported not-ok")
			}
			// bytes.Equal treats nil and an empty slice as equal, so the allow-all case
			// (nil in, nil out) passes the same check as the byte-for-byte cases.
			if !bytes.Equal(got, c.blob) {
				t.Fatalf("round-trip mismatch: %q -> %q", c.blob, got)
			}
		})
	}
}

// TestPackRobotsPicksSmaller checks the packer never stores a compressed form
// that is not actually smaller than raw, so a short blob that zstd would inflate
// stays raw.
func TestPackRobotsPicksSmaller(t *testing.T) {
	blob := []byte("Disallow: /q")
	packed := PackRobots(blob, CodecZstd)
	if packed[0] != robotsRaw {
		t.Fatalf("short blob packed as mode %d, want raw", packed[0])
	}
	if len(packed) != len(blob)+1 {
		t.Fatalf("raw packing is %d bytes, want blob+1 = %d", len(packed), len(blob)+1)
	}
}

// TestUnpackRobotsRejectsCorrupt checks a malformed packed blob reports not-ok
// rather than returning garbage or panicking: an empty input, an unknown mode,
// an allow-all blob with a stray body, and a compressed body that is not valid
// codec output.
func TestUnpackRobotsRejectsCorrupt(t *testing.T) {
	bad := [][]byte{
		{},
		{9, 1, 2, 3},             // unknown mode
		{robotsAllowAll, 1},      // allow-all carries no body
		{robotsCompressed, 0, 1}, // not valid zstd
	}
	for i, b := range bad {
		if _, ok := UnpackRobots(b, CodecZstd, RobotsSizeHint); ok {
			t.Fatalf("corrupt input %d unpacked ok, want not-ok", i)
		}
	}
}
