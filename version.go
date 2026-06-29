package meguri

// Build stamps, set by the linker at release time (see the Makefile and the
// GoReleaser config). They default to dev values for a plain `go build`.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)
