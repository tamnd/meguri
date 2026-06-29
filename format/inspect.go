package format

import (
	"fmt"
	"strings"
)

// RegionInfo is one row of the inspect region table.
type RegionInfo struct {
	ID     uint8
	Name   string
	Offset uint64
	Length uint64
}

// Inspect is the cheap structural summary of a .meguri file: everything the
// header and footer carry, with no column data decoded. It is what the inspect
// subcommand prints and what a tool reaches for to decide whether a file is
// worth loading. It is read from the tail, so it costs a header read plus a
// footer read regardless of file size.
type Inspect struct {
	PartitionID  uint32
	VersionMajor uint16
	VersionMinor uint16
	Flags        uint16
	ChecksumAlgo uint8
	DefaultCodec uint8
	CreatedHours uint32
	HostKeyLo    uint64
	HostKeyHi    uint64
	URLCount     uint64
	HostCount    uint64
	FileSize     uint64

	Regions     []RegionInfo
	URLColumns  int
	HostColumns int
	Encodings   map[string]int // encoding name -> column count, the cascade made visible
	Stats       Stats
	Meta        map[string]string
}

// Stats is the inspect-visible copy of the footer stats block.
type Stats struct {
	ScheduledCount    uint64
	DueMin            uint32
	DueMax            uint32
	TotalCompressed   uint64
	TotalUncompressed uint64
	BytesPerURL       float32
}

var regionNames = map[uint8]string{
	RegionURLTable:   "url_table",
	RegionHostTable:  "host_table",
	RegionSchedule:   "schedule",
	RegionSeenset:    "seenset",
	RegionStringBlob: "string_blob",
}

// InspectBytes builds an Inspect from a whole file in memory. It verifies the
// header and footer checksums but does not touch the column pages.
func InspectBytes(b []byte) (*Inspect, error) {
	if len(b) < HeaderSize+trailerSize {
		return nil, ErrShortFile
	}
	h, err := DecodeHeader(b[:HeaderSize])
	if err != nil {
		return nil, err
	}
	if [4]byte(b[len(b)-4:]) != Magic {
		return nil, ErrBadMagic
	}
	r := &rbuf{b: b[len(b)-trailerSize:]}
	footerLen := int(r.u32())
	footerCRC := r.u32()
	footerStart := len(b) - trailerSize - footerLen
	if footerStart < HeaderSize || footerStart != int(h.FooterOffset) {
		return nil, ErrCorrupt
	}
	footerBytes := b[footerStart : len(b)-trailerSize]
	if crc32c(footerBytes) != footerCRC {
		return nil, ErrChecksum
	}
	f, err := decodeFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	ins := &Inspect{
		PartitionID:  h.PartitionID,
		VersionMajor: h.VersionMajor,
		VersionMinor: h.VersionMinor,
		Flags:        h.Flags,
		ChecksumAlgo: h.ChecksumAlgo,
		DefaultCodec: h.DefaultCodec,
		CreatedHours: h.CreatedHours,
		HostKeyLo:    h.HostKeyLo,
		HostKeyHi:    h.HostKeyHi,
		URLCount:     h.URLCount,
		HostCount:    h.HostCount,
		FileSize:     uint64(len(b)),
		URLColumns:   len(f.urlDir),
		HostColumns:  len(f.hostDir),
		Meta:         metaMap(f.meta),
		Stats: Stats{
			ScheduledCount:    f.stats.scheduledCount,
			DueMin:            f.stats.dueMin,
			DueMax:            f.stats.dueMax,
			TotalCompressed:   f.stats.totalCompressed,
			TotalUncompressed: f.stats.totalUncompressed,
			BytesPerURL:       f.stats.bytesPerURL,
		},
	}
	for _, reg := range f.regions {
		ins.Regions = append(ins.Regions, RegionInfo{
			ID:     reg.id,
			Name:   regionName(reg.id),
			Offset: reg.offset,
			Length: reg.length,
		})
	}
	ins.Encodings = map[string]int{}
	for _, d := range f.urlDir {
		ins.Encodings[encodingName(d.encoding)]++
	}
	for _, d := range f.hostDir {
		ins.Encodings[encodingName(d.encoding)]++
	}
	return ins, nil
}

func encodingName(e uint8) string {
	switch e {
	case EncRaw:
		return "raw"
	case EncDict:
		return "dict"
	case EncDelta:
		return "delta"
	case EncFOR:
		return "for"
	case EncRLE:
		return "rle"
	case EncFSST:
		return "fsst"
	case EncDeltaFOR:
		return "delta_for"
	default:
		return fmt.Sprintf("enc_%d", e)
	}
}

func regionName(id uint8) string {
	if n, ok := regionNames[id]; ok {
		return n
	}
	return fmt.Sprintf("region_%d", id)
}

// String renders the inspect summary as a stable, human-readable block.
func (ins *Inspect) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "meguri v%d.%d  partition %d\n", ins.VersionMajor, ins.VersionMinor, ins.PartitionID)
	fmt.Fprintf(&b, "  file size      %d bytes\n", ins.FileSize)
	fmt.Fprintf(&b, "  hostkey range  0x%016x .. 0x%016x\n", ins.HostKeyLo, ins.HostKeyHi)
	fmt.Fprintf(&b, "  urls           %d\n", ins.URLCount)
	fmt.Fprintf(&b, "  hosts          %d\n", ins.HostCount)
	fmt.Fprintf(&b, "  url columns    %d\n", ins.URLColumns)
	fmt.Fprintf(&b, "  host columns   %d\n", ins.HostColumns)
	fmt.Fprintf(&b, "  checksum       %s\n", checksumName(ins.ChecksumAlgo))
	fmt.Fprintf(&b, "  default codec  %s\n", codecName(ins.DefaultCodec))
	fmt.Fprintf(&b, "  created        %d epoch-hours\n", ins.CreatedHours)
	fmt.Fprintf(&b, "  flags          %s\n", flagNames(ins.Flags))
	if ins.URLCount > 0 {
		fmt.Fprintf(&b, "  bytes/url      %.2f\n", ins.Stats.BytesPerURL)
	}
	if ins.Stats.DueMax > 0 {
		fmt.Fprintf(&b, "  next-due range %d .. %d epoch-hours\n", ins.Stats.DueMin, ins.Stats.DueMax)
	}
	b.WriteString("  regions:\n")
	for _, reg := range ins.Regions {
		fmt.Fprintf(&b, "    %-12s off=%-10d len=%d\n", reg.Name, reg.Offset, reg.Length)
	}
	if len(ins.Encodings) > 0 {
		b.WriteString("  encodings:\n")
		for _, k := range sortedCounts(ins.Encodings) {
			fmt.Fprintf(&b, "    %-12s %d cols\n", k, ins.Encodings[k])
		}
	}
	if len(ins.Meta) > 0 {
		b.WriteString("  meta:\n")
		for _, k := range sortedKeys(ins.Meta) {
			fmt.Fprintf(&b, "    %s = %s\n", k, ins.Meta[k])
		}
	}
	return b.String()
}

func checksumName(a uint8) string {
	switch a {
	case ChecksumNone:
		return "none"
	case ChecksumCRC32C:
		return "crc32c"
	case ChecksumXXH64:
		return "xxh64"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

func codecName(c uint8) string {
	switch c {
	case CodecNone:
		return "none"
	case CodecLZ4:
		return "lz4"
	case CodecZstd:
		return "zstd"
	case CodecZstdDict:
		return "zstd-dict"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

func flagNames(flags uint16) string {
	if flags == 0 {
		return "none"
	}
	var on []string
	for _, f := range []struct {
		bit  uint16
		name string
	}{
		{FlagSorted, "sorted"},
		{FlagHasSchedule, "schedule"},
		{FlagHasSeenset, "seenset"},
		{FlagHasBlob, "blob"},
		{FlagSeensetIsRibbon, "ribbon"},
		{FlagHasMPHF, "mphf"},
		{FlagFooterCompressed, "footer-zstd"},
	} {
		if flags&f.bit != 0 {
			on = append(on, f.name)
		}
	}
	return strings.Join(on, "|")
}

func sortedCounts(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
