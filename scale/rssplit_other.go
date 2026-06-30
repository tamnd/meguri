//go:build !linux

package scale

// ReadRSSSplit is the fallback for non-Linux boxes. The anon/file split lives in
// /proc/self/status, which only Linux exposes; the dev box is darwin and the boxes
// of record are Linux, so off Linux this reports "not captured" rather than a
// wrong number, and the run still records total peak RSS through getrusage.
func ReadRSSSplit() RSSSplit { return RSSSplit{} }
