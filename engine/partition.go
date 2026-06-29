package engine

import (
	"github.com/tamnd/meguri/format"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/store"
)

// Partition is the top-level open/close lifecycle of one durable partition: open
// a directory, get a running frontier backed by the log-structured store, advance
// it through an Engine, and fold it back to a durable checkpoint on close (doc 11,
// doc 04). It is the handle the serve command holds and the library entry point a
// host process embeds: the store is the durable home and the frontier is the
// resident scheduler, and the lifecycle keeps the two in step so a caller never
// touches the store and the frontier separately.
//
// The split of responsibilities: store.Open recovers the durable snapshot and log
// tail, Snapshot hands that state to frontier.Recover as a resident schedule, the
// Engine advances the frontier, and Checkpoint serializes the advanced frontier
// back through store.CheckpointFrom. During a session the frontier is the live
// truth; the store is only read at Open and only written at Checkpoint, so the two
// never race and the durable form is always a clean cut between engine ticks.
type Partition struct {
	dir string
	st  *store.Store
	fr  *frontier.Frontier
}

// OpenPartition opens or recovers the durable partition rooted at dir and returns
// a handle whose frontier is ready to drive. A fresh directory comes back with an
// empty frontier to seed; an existing one comes back with its committed schedule
// recovered. The frontier options (WithStateMachine, WithPrioritization, and the
// rest) configure the resident scheduler the same way frontier.Recover does
// directly, so a served partition carries the same policy a one-shot run does.
func OpenPartition(dir string, opts store.Options, frOpts ...frontier.Option) (*Partition, error) {
	st, err := store.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	fr := frontier.Recover(st.Snapshot(), frOpts...)
	return &Partition{dir: dir, st: st, fr: fr}, nil
}

// Frontier returns the resident frontier the lifecycle drives. A caller seeds it,
// hands it to engine.New, and reads it back; the lifecycle owns persisting it.
func (p *Partition) Frontier() *frontier.Frontier { return p.fr }

// Dir is the directory the partition is rooted at, the durable home of its log and
// snapshot.
func (p *Partition) Dir() string { return p.dir }

// Checkpoint folds the live frontier into the store's durable checkpoint: it
// serializes the frontier to its .meguri form and commits it through the store,
// which writes the snapshot, rotates the log, and swaps the superblock. After it
// returns the on-disk partition reflects every advance the engine made, and a
// later OpenPartition recovers exactly this state.
func (p *Partition) Checkpoint() error {
	raw, err := p.fr.CheckpointBytes()
	if err != nil {
		return err
	}
	part, err := format.Decode(raw)
	if err != nil {
		return err
	}
	return p.st.CheckpointFrom(part)
}

// Close checkpoints the live frontier and closes the underlying store, the clean
// shutdown of a served partition. A caller that has already checkpointed and wants
// to drop the session without re-persisting calls Abandon instead.
func (p *Partition) Close() error {
	if err := p.Checkpoint(); err != nil {
		_ = p.st.Close()
		return err
	}
	return p.st.Close()
}

// Abandon closes the store without checkpointing, discarding any frontier advance
// since the last Checkpoint. It is the shutdown path for a read-only session or a
// run whose result the caller chooses not to keep; recovery falls back to the last
// committed checkpoint.
func (p *Partition) Abandon() error { return p.st.Close() }
