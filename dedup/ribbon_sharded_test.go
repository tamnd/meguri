package dedup

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/tamnd/meguri"
)

// partitionKeys splits keys into s shards the way the seal path does, routing each
// key by RibbonShardIndex with the same count the query path will use.
func partitionKeys(keys []meguri.URLKey, s int) [][]meguri.URLKey {
	shards := make([][]meguri.URLKey, s)
	for _, k := range keys {
		i := RibbonShardIndex(k, s)
		shards[i] = append(shards[i], k)
	}
	return shards
}

// TestShardedRibbonNoFalseNegative is the one-sided guarantee for the sharded form:
// every key built into any shard must probe true through the combined filter, at each
// shard count, because a false negative would drop a genuinely seen url.
func TestShardedRibbonNoFalseNegative(t *testing.T) {
	for _, s := range []int{2, 4, 8, 16, 64} {
		keys := makeKeys(20000)
		blob, err := BuildShardedRibbonFilter(partitionKeys(keys, s))
		if err != nil {
			t.Fatalf("s=%d build: %v", s, err)
		}
		if blob[1] != filterKindShardedRibbon {
			t.Fatalf("s=%d blob kind = %d, want sharded ribbon (%d)", s, blob[1], filterKindShardedRibbon)
		}
		rf, err := UnmarshalFilter(blob)
		if err != nil {
			t.Fatalf("s=%d unmarshal: %v", s, err)
		}
		if rf.Len() != uint64(len(keys)) {
			t.Fatalf("s=%d filter holds %d keys, want %d", s, rf.Len(), len(keys))
		}
		for i, k := range keys {
			if !rf.MaybeContains(k) {
				t.Fatalf("s=%d: key %d false-negative, the one-sided contract broke", s, i)
			}
		}
	}
}

// TestShardedRibbonRoundTrip checks the sharded blob reconstructs a filter that holds
// every member and keeps the false-positive rate near 2^-r, so sharding does not
// change the membership contract, only how the seal is solved.
func TestShardedRibbonRoundTrip(t *testing.T) {
	keys := makeKeys(50000)
	blob, err := BuildShardedRibbonFilter(partitionKeys(keys, 8))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	rf, err := UnmarshalFilter(blob)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for i, k := range keys {
		if !rf.MaybeContains(k) {
			t.Fatalf("member %d went missing after round trip", i)
		}
	}
	absent := absentKeys(200000)
	fp := 0
	for _, k := range absent {
		if rf.MaybeContains(k) {
			fp++
		}
	}
	rate := float64(fp) / float64(len(absent))
	want := 1.0 / float64(int(1)<<defaultRibbonR)
	if rate > want*2 {
		t.Fatalf("false-positive rate %.4f exceeds 2x the target %.4f", rate, want)
	}
	t.Logf("sharded ribbon: %d keys over 8 shards, %d blob bytes (%.2f bits/url), fp rate %.4f (target %.4f)",
		len(keys), len(blob), rf.BitsPerURL(), rate, want)
}

// TestShardedRibbonSingleShardIsKind1 pins the byte-compatibility rule: a one-shard
// build emits exactly the kind-1 blob BuildRibbonFilter would, so small seals stay
// identical to the single ribbon form and existing readers are unaffected.
func TestShardedRibbonSingleShardIsKind1(t *testing.T) {
	keys := makeKeys(1000)
	shardedBlob, err := BuildShardedRibbonFilter(partitionKeys(keys, 1))
	if err != nil {
		t.Fatalf("sharded build: %v", err)
	}
	if shardedBlob[1] != filterKindRibbon {
		t.Fatalf("one-shard blob kind = %d, want kind-1 ribbon (%d)", shardedBlob[1], filterKindRibbon)
	}
	singleBlob, err := BuildRibbonFilter(keys)
	if err != nil {
		t.Fatalf("single build: %v", err)
	}
	if !bytes.Equal(shardedBlob, singleBlob) {
		t.Fatalf("one-shard blob differs from the single ribbon blob (%d vs %d bytes)", len(shardedBlob), len(singleBlob))
	}
}

// TestRibbonShardCount pins the size-to-shard-count mapping the seal picks: small
// seals stay a single shard, larger seals split into a power of two that keeps each
// shard near the target, and the fan-out is capped.
func TestRibbonShardCount(t *testing.T) {
	cases := []struct {
		n    int
		want int
	}{
		{0, 1},
		{1, 1},
		{ribbonShardTarget, 1},
		{ribbonShardTarget + 1, 2},
		{ribbonShardTarget * 3, 4},
		{100_000_000, 512},
		{2_000_000_000, maxRibbonShards},
	}
	for _, c := range cases {
		if got := RibbonShardCount(c.n); got != c.want {
			t.Fatalf("RibbonShardCount(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

// TestRibbonShardIndexRange checks routing stays inside the shard range, is stable
// across calls, and collapses to shard 0 when there is a single shard.
func TestRibbonShardIndexRange(t *testing.T) {
	keys := makeKeys(10000)
	for _, s := range []int{1, 2, 8, 512, 4096} {
		for _, k := range keys {
			i := RibbonShardIndex(k, s)
			if i < 0 || i >= s {
				t.Fatalf("s=%d: index %d out of range", s, i)
			}
			if i != RibbonShardIndex(k, s) {
				t.Fatalf("s=%d: index not stable across calls", s)
			}
		}
	}
	for _, k := range keys {
		if RibbonShardIndex(k, 1) != 0 {
			t.Fatal("single shard must route to 0")
		}
	}
}

// TestShardedRibbonRejectsGarbage checks the reader refuses a truncated or internally
// inconsistent sharded blob rather than panicking or fabricating a filter.
func TestShardedRibbonRejectsGarbage(t *testing.T) {
	good, err := BuildShardedRibbonFilter(partitionKeys(makeKeys(5000), 4))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	oversizeShards := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(oversizeShards[4:8], maxRibbonShards+1)

	shortIndex := append([]byte(nil), good[:shardedRibbonHeaderSize+2]...) // header plus a partial index

	badBody := append([]byte(nil), good...)
	badBody = badBody[:len(badBody)-1] // one byte short of the declared sub-blob lengths

	cases := map[string][]byte{
		"empty":                {},
		"header only":          good[:shardedRibbonHeaderSize],
		"bad version":          append([]byte{0xFF}, good[1:]...),
		"oversize shard count": oversizeShards,
		"short index":          shortIndex,
		"truncated body":       badBody,
	}
	for name, blob := range cases {
		if _, err := UnmarshalFilter(blob); err == nil {
			t.Fatalf("%s: UnmarshalFilter accepted a malformed sharded blob", name)
		}
	}
}
