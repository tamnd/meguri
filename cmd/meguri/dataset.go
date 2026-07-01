package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tamnd/meguri/dataset"
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/live"
)

// newDatasetCmd is the two-way bridge between a .meguri store and Apache Parquet
// (spec 2073 doc 08, publishing the store). It carries two subcommands: export turns
// a file or a sharded store into Parquet, either one .parquet or a Hugging Face
// dataset repo folder that grows one commit per incremental dump; import rebuilds a
// single .meguri from a Parquet file or such a repo, merging an incremental dataset
// back to the latest state of each URL. The pair is lossless: a record survives the
// trip out and back unchanged.
func newDatasetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dataset",
		Short: "Convert a .meguri store to and from Parquet for publishing",
		Long:  "dataset bridges a .meguri store and Apache Parquet. export writes a single .parquet or a Hugging Face dataset repo folder (data/*.parquet plus a card and manifest) that takes one commit per incremental dump; import rebuilds a .meguri from a Parquet file or repo, merging incremental dumps to the newest copy of each URL.",
	}
	cmd.AddCommand(newDatasetExportCmd())
	cmd.AddCommand(newDatasetImportCmd())
	return cmd
}

// newDatasetExportCmd streams a source to Parquet. --repo picks the publish-ready
// folder shape; without it the export is a single file. --since carries the prior
// dump's watermark so an incremental export writes only what is new or changed, and
// against an existing repo the new files append rather than replace.
func newDatasetExportCmd() *cobra.Command {
	var (
		src      string
		out      string
		repo     bool
		codec    string
		rowGroup int
		fileRows int
		since    uint32
		asJSON   bool
	)
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export a .meguri file or store directory to Parquet",
		Long:  "export reads --src (a .meguri file or a sharded store directory) and writes Parquet to --out. With --repo, --out is a Hugging Face dataset repo folder (data/*.parquet plus README.md and manifest.json) that a second dump grows as a commit; without it, --out is one .parquet file. --since h exports only rows with activity at or after epoch-hour h, the incremental cursor a prior dump's reported watermark feeds.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if src == "" {
				return fmt.Errorf("--src is required")
			}
			if out == "" {
				return fmt.Errorf("--out is required")
			}
			opts := dataset.ExportOptions{
				RowGroupRows: rowGroup,
				FileRows:     fileRows,
				Codec:        codec,
				SinceHours:   since,
			}
			var (
				st  dataset.ExportStats
				err error
			)
			if repo {
				st, err = dataset.ExportRepo(src, out, opts)
			} else {
				st, err = dataset.ExportSingle(src, out, opts)
			}
			if err != nil {
				return err
			}
			return reportExport(cmd, out, repo, st, asJSON)
		},
	}
	cmd.Flags().StringVar(&src, "src", "", "source .meguri file or store directory (required)")
	cmd.Flags().StringVar(&out, "out", "", "output .parquet file, or repo folder with --repo (required)")
	cmd.Flags().BoolVar(&repo, "repo", false, "write a Hugging Face dataset repo folder instead of a single .parquet")
	cmd.Flags().StringVar(&codec, "codec", "zstd", "column compression: zstd, snappy, or none")
	cmd.Flags().IntVar(&rowGroup, "row-group", 0, "rows per Parquet row group (0 uses the default)")
	cmd.Flags().IntVar(&fileRows, "file-rows", 0, "rows per data file in --repo mode (0 uses the default)")
	cmd.Flags().Uint32Var(&since, "since", 0, "export only rows with activity at or after this epoch-hour (0 exports all)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the export stats as JSON")
	return cmd
}

// newDatasetImportCmd rebuilds a .meguri from Parquet. The input is one .parquet file
// or a dataset repo folder; a repo's manifest names its files in append order, so an
// incremental dataset imports with a later dump winning a key tie and no duplicate row.
func newDatasetImportCmd() *cobra.Command {
	var (
		in     string
		out    string
		codec  string
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import a Parquet file or dataset repo into a .meguri file",
		Long:  "import reads --in (a single .parquet file or a dataset repo folder whose manifest.json names the data files) and writes a single .meguri to --out. Across an incremental dataset the rows merge in URLKey order with the newest dump winning a tie, so a changed URL resolves to its latest state and no duplicate survives.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if in == "" {
				return fmt.Errorf("--in is required")
			}
			if out == "" {
				return fmt.Errorf("--out is required")
			}
			mc, err := meguriCodec(codec)
			if err != nil {
				return err
			}
			res, err := dataset.Import(in, out, dataset.ImportOptions{Codec: mc})
			if err != nil {
				return err
			}
			return reportImport(cmd, out, res, asJSON)
		},
	}
	cmd.Flags().StringVar(&in, "in", "", "input .parquet file or dataset repo folder (required)")
	cmd.Flags().StringVar(&out, "out", "", "output .meguri file (required)")
	cmd.Flags().StringVar(&codec, "codec", "zstd", "output .meguri body codec: zstd or none")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the import result as JSON")
	return cmd
}

// meguriCodec maps a codec name to the format body codec constant. It is the .meguri
// codec, distinct from the Parquet column codec the export chooses.
func meguriCodec(name string) (uint8, error) {
	switch name {
	case "", "zstd":
		return format.CodecZstd, nil
	case "none", "raw", "uncompressed":
		return format.CodecNone, nil
	default:
		return 0, fmt.Errorf("unknown meguri codec %q (want zstd or none)", name)
	}
}

// reportExport prints the export stats, as JSON when asked or as a short human report.
func reportExport(cmd *cobra.Command, out string, repo bool, st dataset.ExportStats, asJSON bool) error {
	w := cmd.OutOrStdout()
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	shape := "single-file"
	if repo {
		shape = "repo"
	}
	if _, err := fmt.Fprintf(w, "exported %s (%s)\n", out, shape); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  rows        %d\n", st.Rows); err != nil {
		return err
	}
	if st.Skipped > 0 {
		if _, err := fmt.Fprintf(w, "  skipped     %d (below --since)\n", st.Skipped); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "  hosts       %d\n", st.Hosts); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  files       %d\n", st.Files); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  watermark   hour %d\n", st.Watermark); err != nil {
		return err
	}
	if st.LossyETags > 0 {
		if _, err := fmt.Fprintf(w, "  lossy etags %d\n", st.LossyETags); err != nil {
			return err
		}
	}
	return nil
}

// reportImport prints the build result of an import.
func reportImport(cmd *cobra.Command, out string, res live.BuildResult, asJSON bool) error {
	w := cmd.OutOrStdout()
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if _, err := fmt.Fprintf(w, "imported %s\n", out); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  urls        %d\n", res.URLCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  hosts       %d\n", res.HostCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  bytes       %d\n", res.FileBytes); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  bits/url    %.2f\n", res.BitsPerURL); err != nil {
		return err
	}
	return nil
}
