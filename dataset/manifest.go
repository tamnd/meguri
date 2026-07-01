package dataset

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// ManifestName is the fixed filename of the dataset manifest at the repo root.
const ManifestName = "manifest.json"

// Manifest describes a published dataset: the schema version, the codec, the row and
// host totals, the incremental watermark, and the list of data files. It is the
// machine-readable index a re-import reads to find every .parquet file, and the record
// an incremental dump advances. It lives beside the data so the folder is
// self-describing without the .meguri it came from.
type Manifest struct {
	SchemaVersion int    `json:"schema_version"`
	Codec         string `json:"codec"`
	RowGroupRows  int    `json:"row_group_rows"`
	FileRows      int    `json:"file_rows"`

	Rows       int64 `json:"rows"`
	Hosts      int   `json:"hosts"`
	LossyETags int64 `json:"lossy_etags,omitempty"`

	// Watermark is the max last_changed epoch-hour across all rows, the cursor the next
	// incremental dump exports past. Zero means nothing has been observed to change yet.
	Watermark uint32 `json:"watermark_hours"`

	Files []FileMeta `json:"files"`
}

// newManifest builds the manifest for a completed export.
func newManifest(st ExportStats, opts ExportOptions, files []FileMeta) Manifest {
	codec := opts.Codec
	if codec == "" {
		codec = "zstd"
	}
	return Manifest{
		SchemaVersion: SchemaVersion,
		Codec:         codec,
		RowGroupRows:  opts.RowGroupRows,
		FileRows:      opts.FileRows,
		Rows:          st.Rows,
		Hosts:         st.Hosts,
		LossyETags:    st.LossyETags,
		Watermark:     st.Watermark,
		Files:         files,
	}
}

// writeManifest writes the manifest to the dataset root.
func writeManifest(dir string, man Manifest) error {
	b, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(dir, ManifestName), b, 0o644)
}

// ReadManifest reads the manifest from a dataset root.
func ReadManifest(dir string) (Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return Manifest{}, err
	}
	var man Manifest
	if err := json.Unmarshal(b, &man); err != nil {
		return Manifest{}, err
	}
	return man, nil
}

// DataFiles returns the absolute paths of every .parquet file the manifest lists, in
// name order, the read set a full import consumes.
func (man Manifest) DataFiles(root string) []string {
	names := make([]FileMeta, len(man.Files))
	copy(names, man.Files)
	sort.Slice(names, func(i, j int) bool { return names[i].Name < names[j].Name })
	paths := make([]string, len(names))
	for i, f := range names {
		paths[i] = filepath.Join(root, f.Name)
	}
	return paths
}

// mergeIncremental folds a fresh dump's files into the prior manifest: the new files
// append, the totals and watermark advance, and the file list is deduplicated by name
// so a re-run over the same slice does not double-count. This is what makes a repo a
// growing dataset rather than a one-shot dump.
func mergeIncremental(prev, next Manifest) Manifest {
	byName := make(map[string]FileMeta, len(prev.Files)+len(next.Files))
	order := make([]string, 0, len(prev.Files)+len(next.Files))
	add := func(files []FileMeta) {
		for _, f := range files {
			if _, ok := byName[f.Name]; !ok {
				order = append(order, f.Name)
			}
			byName[f.Name] = f
		}
	}
	add(prev.Files)
	add(next.Files)

	merged := next
	merged.Files = make([]FileMeta, 0, len(order))
	var rows int64
	for _, name := range order {
		f := byName[name]
		merged.Files = append(merged.Files, f)
		rows += f.Rows
	}
	merged.Rows = rows
	if prev.Hosts > merged.Hosts {
		merged.Hosts = prev.Hosts
	}
	if prev.Watermark > merged.Watermark {
		merged.Watermark = prev.Watermark
	}
	merged.LossyETags += prev.LossyETags
	return merged
}
