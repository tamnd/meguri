package engine

import (
	"os"

	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// Collection is the fleet-side reader of a partition manifest: the catalog that
// binds many .meguri partitions into one frontier (doc 10 section 11). It reads a
// manifest file once and answers the three fleet questions without opening any
// partition: which partition owns a host (Route), which partitions have due work
// (DueParts), and whether the ranges tile the key space cleanly (CoverageGap). A
// serve process holds it to route a discovered link to its owner, and OpenPartition
// opens the durable partition a routed entry points at.
type Collection struct {
	man *format.Manifest
}

// OpenCollection reads and parses a manifest file, the reader half of the fleet
// catalog. The file is what BuildManifest plus EncodeManifest wrote, verified at
// both ends by its magic and CRC, so a torn manifest fails loudly here rather than
// routing to a partition that was never committed.
func OpenCollection(path string) (*Collection, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	man, err := format.DecodeManifest(b)
	if err != nil {
		return nil, err
	}
	return &Collection{man: man}, nil
}

// Manifest returns the parsed catalog, for callers that want the raw entries or the
// CoverageGap and DueParts pushdowns directly.
func (c *Collection) Manifest() *format.Manifest { return c.man }

// Route returns the partition entry whose host-key range contains hk, the lookup a
// router runs to send a discovered link to its owner. ok is false when no partition
// owns hk, a gap a control plane treats as missing coverage.
func (c *Collection) Route(hk uint64) (format.ManifestEntry, bool) {
	return c.man.Route(hk)
}

// OpenPartition opens the durable partition a manifest entry points at, resolving
// the entry's FileRef as a store directory. It is how a routed lookup turns into a
// live handle: route a host to its entry, open that entry, drive its frontier.
func (c *Collection) OpenPartition(e format.ManifestEntry, opts store.Options, frOpts ...frontier.Option) (*Partition, error) {
	return OpenPartition(e.FileRef, opts, frOpts...)
}

// ManifestEntry derives this partition's manifest row from its live frontier: it
// serializes the frontier to its .meguri form and reads the header and footer the
// catalog records (range, counts, soonest due, CRC). The FileRef is the partition's
// own directory, the durable home a Collection reopens it from. A control plane
// collects one entry per partition into BuildManifest to publish the fleet catalog.
func (p *Partition) ManifestEntry(epoch uint32) (format.ManifestEntry, error) {
	raw, err := p.fr.CheckpointBytes()
	if err != nil {
		return format.ManifestEntry{}, err
	}
	return format.ManifestEntryFor(raw, p.dir, epoch)
}
