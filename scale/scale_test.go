package scale

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCountingWriterTallies(t *testing.T) {
	var buf bytes.Buffer
	cw := &CountingWriter{W: &buf}
	for _, s := range []string{"abc", "de", ""} {
		if _, err := cw.Write([]byte(s)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if cw.N != 5 {
		t.Fatalf("counted %d bytes, want 5", cw.N)
	}
	if buf.String() != "abcde" {
		t.Fatalf("passthrough = %q, want abcde", buf.String())
	}
}

func TestSyncTimerSummary(t *testing.T) {
	var st SyncTimer
	if n, _, _ := st.Summary(); n != 0 {
		t.Fatalf("empty summary count = %d, want 0", n)
	}
	for i := 0; i < 5; i++ {
		if err := st.Time(func() error { return nil }); err != nil {
			t.Fatalf("time: %v", err)
		}
	}
	n, p50, p99 := st.Summary()
	if n != 5 {
		t.Fatalf("count = %d, want 5", n)
	}
	if p50 < 0 || p99 < 0 {
		t.Fatalf("negative latency p50=%f p99=%f", p50, p99)
	}
}

func TestSyncTimerPropagatesError(t *testing.T) {
	var st SyncTimer
	want := errors.New("flush failed")
	if err := st.Time(func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("Time err = %v, want %v", err, want)
	}
	if n, _, _ := st.Summary(); n != 1 {
		t.Fatalf("a failed sync is still timed, count = %d, want 1", n)
	}
}

func TestSamplerObservesHeap(t *testing.T) {
	s := NewSampler(time.Millisecond)
	s.Start()
	sink := make([][]byte, 0, 64)
	for i := 0; i < 64; i++ {
		sink = append(sink, make([]byte, 1<<16))
	}
	time.Sleep(5 * time.Millisecond)
	peak := s.Stop()
	if peak == 0 {
		t.Fatal("sampler observed no heap")
	}
	_ = sink
}

func TestRequireMeasurableRejectsSmoke(t *testing.T) {
	smoke := Result{Profile: "10k"}
	if err := smoke.requireMeasurable(); err == nil {
		t.Fatal("a run with no box and no corpus must not be measurable")
	}
	noBox := Result{Provenance: Provenance{Corpus: "corpus/urls.jsonl"}}
	if err := noBox.requireMeasurable(); err == nil {
		t.Fatal("a run with no box must not be measurable")
	}
	ok := Result{Provenance: Provenance{Box: "server2", Corpus: "corpus/urls.jsonl"}}
	if err := ok.requireMeasurable(); err != nil {
		t.Fatalf("a boxed real-corpus run must be measurable, got %v", err)
	}
}

func TestStageMetricsCountsWork(t *testing.T) {
	res, err := StageResultFromSeed(1000, func() (uint64, error) {
		sink := make([]int, 0, 1000)
		for i := 0; i < 1000; i++ {
			sink = append(sink, i)
		}
		_ = sink
		return 4096, nil
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if res.Stage != "seed" {
		t.Fatalf("stage = %q, want seed", res.Stage)
	}
	if res.URLs != 1000 {
		t.Fatalf("urls = %d, want 1000", res.URLs)
	}
	if res.Disk.OutputBytes != 4096 {
		t.Fatalf("output bytes = %d, want 4096", res.Disk.OutputBytes)
	}
	if res.AllocBytesPerURL <= 0 {
		t.Fatal("alloc per url should be positive")
	}
}

func TestStageMetricsStampsIO(t *testing.T) {
	res, err := StageResultFromSeed(1000, func() (uint64, error) {
		// Touch a few MB so the stage takes some minor faults growing the heap;
		// the exact counts are platform-charged, so the assertion only checks the
		// delta is a sane non-underflowed value, not a specific number.
		sink := make([][]byte, 0, 256)
		for range 256 {
			sink = append(sink, make([]byte, 4096))
		}
		_ = sink
		return 0, nil
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	// getrusage counters only grow, so a stage delta can never exceed a plausible
	// ceiling for a few-MB workload; a wild value would mean the delta underflowed
	// (subtracting a larger cumulative from a smaller one).
	if res.IO.MajorFaults > 1<<40 || res.IO.MinorFaults > 1<<40 {
		t.Fatalf("IO delta looks underflowed: major=%d minor=%d", res.IO.MajorFaults, res.IO.MinorFaults)
	}
}

func TestStageResultJSONOmitsEmptyIOAndLatency(t *testing.T) {
	// A stage with no IO and no hot op drops both keys, so a reader never sees an
	// empty io block imply IO happened or a null latency imply a measured op.
	bare, err := json.Marshal(StageResult{Stage: "seed", URLs: 10})
	if err != nil {
		t.Fatalf("marshal bare: %v", err)
	}
	if s := string(bare); strings.Contains(s, "\"io\"") || strings.Contains(s, "\"latency\"") {
		t.Fatalf("bare stage should omit io and latency, got %s", s)
	}
	// A stage that took faults and measured a hot op carries both, machine-readably.
	full := StageResult{
		Stage: "live-schedule",
		URLs:  100,
		IO:    IOSummary{MajorFaults: 12107, MinorFaults: 900},
		Latency: &LatencySummary{
			Op: "NextBatch", Samples: 1536, P50Ns: 4096, P99Ns: 65536, MaxNs: 131072,
		},
	}
	out, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshal full: %v", err)
	}
	for _, want := range []string{"\"major_faults\":12107", "\"op\":\"NextBatch\"", "\"p99_ns\":65536"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("full stage json missing %q, got %s", want, out)
		}
	}
}

func TestResultWriteHumanFlagsSmoke(t *testing.T) {
	var buf bytes.Buffer
	r := Result{Profile: "10k", Stages: []StageResult{{Stage: "seed", URLs: 10}}}
	r.WriteHuman(&buf)
	if !strings.Contains(buf.String(), "SMOKE") {
		t.Fatalf("human report should flag a boxless run as SMOKE, got:\n%s", buf.String())
	}
}
