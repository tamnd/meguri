package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	meguri "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/seed"
)

// newSeedpackCmd converts a JSONL (or plaintext-URL) corpus into N hostkey-range
// binary seed shards plus a manifest, the Spec 2074 doc 08 input layer. It is the
// one place the corpus is JSON-parsed; every later pass reads the binary seed with
// no gzip and no JSON.
//
// The shards tile the uint64 hostkey space in equal-width ranges and a URL routes to
// its shard by the top bits of its hostkey. Because the hostkey is a uniform hash of
// the host, equal-width ranges hold near-equal URL counts, and a host maps to exactly
// one hostkey so its URLs never split across shards. The input corpus is sorted by
// host string, not by hostkey hash, so the passes cannot assume a sorted stream: all
// shard writers stay open and each URL is dispatched to the writer its hostkey selects.
func newSeedpackCmd() *cobra.Command {
	var (
		input     string
		out       string
		shards    int
		blockSize int
		codecName string
	)
	cmd := &cobra.Command{
		Use:   "seedpack",
		Short: "Convert a JSONL/plaintext URL corpus into sharded binary .mgs seeds",
		Long:  "seedpack reads a URL corpus (JSONL {\"url\":...} or one URL per line, optionally gzipped) and writes N hostkey-range .mgs shard seeds plus a manifest, the splittable parse-free input for the sharded parallel passes. Shards tile the hostkey space in equal-width ranges, which are near equal-count because the hostkey is a uniform hash of the host.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if out == "" {
				return fmt.Errorf("--out is required")
			}
			if shards < 1 {
				return fmt.Errorf("--shards must be >= 1")
			}
			var codec seed.Codec
			switch codecName {
			case "raw", "":
				codec = seed.CodecRaw
			case "zstd":
				codec = seed.CodecZstd
			default:
				return fmt.Errorf("--codec must be raw or zstd")
			}
			if err := os.MkdirAll(out, 0o755); err != nil {
				return err
			}
			return runSeedpack(cmd.OutOrStdout(), input, out, shards, blockSize, codec)
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "corpus path (.jsonl, .gz, or plaintext); stdin if empty")
	cmd.Flags().StringVar(&out, "out", "", "output directory for the shard seeds and manifest")
	cmd.Flags().IntVar(&shards, "shards", 32, "number of hostkey-range shards; rounded up to a power of two")
	cmd.Flags().IntVar(&blockSize, "block-size", seed.DefaultBlockSize, "seed block size in bytes")
	cmd.Flags().StringVar(&codecName, "codec", "raw", "block codec: raw or zstd")
	return cmd
}

// shardBits returns the smallest b with 2^b >= shards, so the shard count rounds up
// to a power of two and the top b bits of a hostkey select the shard.
func shardBits(shards int) int {
	b := 0
	for (1 << b) < shards {
		b++
	}
	return b
}

// runSeedpack streams the corpus once, dispatching each URL to the shard writer its
// hostkey selects, then writes the manifest. Every writer is open at once because the
// corpus is not hostkey-sorted; the resident cost is one block buffer per shard plus
// the growing per-shard block index, not the corpus.
func runSeedpack(stdout io.Writer, input, out string, shards, blockSize int, codec seed.Codec) error {
	in, closeIn, err := openCorpus(input)
	if err != nil {
		return err
	}
	defer func() { _ = closeIn() }()

	bits := shardBits(shards)
	n := 1 << bits // actual shard count, a power of two >= shards
	// shard = hostKey >> (64 - bits); guard bits == 0 (single shard) where the shift
	// would be 64 and undefined.
	shift := uint(64 - bits)
	shardOf := func(hostKey uint64) int {
		if bits == 0 {
			return 0
		}
		return int(hostKey >> shift)
	}
	// Each shard i owns the half-open hostkey range [i<<shift, (i+1)<<shift); the last
	// shard's HostHi is the max so the ranges tile the whole space with no gap.
	hostLo := func(i int) uint64 {
		if bits == 0 {
			return 0
		}
		return uint64(i) << shift
	}
	hostHi := func(i int) uint64 {
		if bits == 0 || i == n-1 {
			return ^uint64(0)
		}
		return uint64(i+1) << shift
	}

	writers := make([]*seed.Writer, n)
	for i := range writers {
		path := filepath.Join(out, fmt.Sprintf("shard-%05d.mgs", i))
		w, err := seed.NewWriter(path, seed.WriterOptions{
			BlockSize: blockSize, Codec: codec, HostLo: hostLo(i), HostHi: hostHi(i),
		})
		if err != nil {
			for _, prev := range writers[:i] {
				_ = prev.Close()
			}
			return err
		}
		writers[i] = w
	}

	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	var total uint64
	for sc.Scan() {
		url := parseCorpusURL(sc.Bytes())
		if url == "" {
			continue
		}
		host := frontier.HostOf(url)
		if host == "" {
			continue
		}
		if err := writers[shardOf(meguri.HostKeyOf(host))].AddString(url); err != nil {
			return err
		}
		total++
	}
	if err := sc.Err(); err != nil {
		return err
	}

	metas := make([]seed.ShardMeta, n)
	for i, w := range writers {
		metas[i] = seed.ShardMeta{
			Index:    i,
			Path:     fmt.Sprintf("shard-%05d.mgs", i),
			HostLo:   hostLo(i),
			HostHi:   hostHi(i),
			Records:  w.Records(),
			URLBytes: w.URLByteCount(),
		}
		if err := w.Close(); err != nil {
			return err
		}
	}

	man := seed.Manifest{
		Version:   1,
		BlockSize: blockSize,
		Codec:     codec,
		Records:   total,
		Shards:    metas,
	}
	if err := seed.WriteManifest(out, man); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "seedpack: %d urls into %d shards under %s (codec=%s)\n",
		total, n, out, codecName(codec))
	for _, m := range metas {
		fmt.Fprintf(stdout, "  shard %05d  %d urls  %.1f MiB urls  hosts [%d,%d)\n",
			m.Index, m.Records, float64(m.URLBytes)/(1<<20), m.HostLo, m.HostHi)
	}
	return nil
}

func codecName(c seed.Codec) string {
	if c == seed.CodecZstd {
		return "zstd"
	}
	return "raw"
}

// openCorpus opens the input, transparently gunzipping a .gz path or a gzip stream.
func openCorpus(input string) (io.Reader, func() error, error) {
	if input == "" {
		return os.Stdin, func() error { return nil }, nil
	}
	f, err := os.Open(input)
	if err != nil {
		return nil, nil, err
	}
	if strings.HasSuffix(input, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		return gz, func() error { _ = gz.Close(); return f.Close() }, nil
	}
	return f, f.Close, nil
}

// parseCorpusURL extracts the URL from one corpus line: a JSON object with a "url"
// field, or a bare URL on plaintext input. It returns "" for a blank or unparseable
// line so the caller skips it.
func parseCorpusURL(line []byte) string {
	s := strings.TrimSpace(string(line))
	if s == "" {
		return ""
	}
	if s[0] == '{' {
		var rec struct {
			URL string `json:"url"`
		}
		if json.Unmarshal([]byte(s), &rec) != nil {
			return ""
		}
		return rec.URL
	}
	return s
}
