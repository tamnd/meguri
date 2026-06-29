// Package distribute is the multi-partition layer: the router that maps a
// HostKey to the partition that owns it, the manifest that records the HostKey
// ranges across the fleet, and the redistribution that splits a hot partition or
// merges cold ones by moving whole .meguri files. It also carries the
// cross-partition discovery transport: a partition that finds a link for a host
// it does not own sends an idempotent meguri.Discovery to the owner, where the
// seen-set absorbs the duplicates an at-least-once transport allows.
//
// This spans the M7 (pack and split/merge), M8 (router), and M9 (replication and
// failover) milestones. The package is a placeholder until then.
package distribute
