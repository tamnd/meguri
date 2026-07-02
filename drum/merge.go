package drum

import (
	"container/heap"

	"github.com/tamnd/meguri"
)

// merge.go is the DRUM's core (spec 2072 doc 04 section 3.4): the batch merge that
// is the dedup check and the index update folded into one sequential sweep. It
// k-way merges the sorted pending runs into one globally URLKey-sorted stream, then
// two-pointer merges that stream against the sorted repository, classifying each
// pending key unique-or-duplicate and writing the merged repository to
// repository.next. The dedup verdict and the index update come from the same
// comparison, so there is no second pass and no second structure.

// Classification is a dedup verdict the discovery path consumes: a key and whether
// the merge found it genuinely new (Unique) or already in the repository.
type Classification struct {
	Key    meguri.URLKey
	Unique bool
}

// pendStream is the k-way merge of all pending runs, yielding pending entries in
// global URLKey order. Each run is sorted, so a min-heap on the runs' head keys
// produces a globally sorted stream with no duplicates lost (a key can repeat
// across runs and within the stream; the merge collapses repeats, section 3.4).
type pendStream struct {
	h *runHeap
}

func newPendStream(runs []*pendRun) *pendStream {
	h := &runHeap{}
	for _, r := range runs {
		if r.more() {
			*h = append(*h, r)
		}
	}
	heap.Init(h)
	return &pendStream{h: h}
}

func (s *pendStream) more() bool { return s.h.Len() > 0 }

// peek returns the lowest-key pending entry without consuming it.
func (s *pendStream) peek() pendEntry { return (*s.h)[0].peek() }

// take consumes the lowest-key pending entry and re-heaps its run.
func (s *pendStream) take() pendEntry {
	r := (*s.h)[0]
	e := r.take()
	if r.more() {
		heap.Fix(s.h, 0)
	} else {
		heap.Pop(s.h)
	}
	return e
}

// runHeap is a min-heap of pending runs ordered by their current head key.
type runHeap []*pendRun

func (h runHeap) Len() int { return len(h) }
func (h runHeap) Less(i, j int) bool {
	a, b := h[i].peek(), h[j].peek()
	if a.key == b.key {
		return a.op < b.op // check-insert before insert-only at the same key
	}
	return a.key.Less(b.key)
}
func (h runHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)   { *h = append(*h, x.(*pendRun)) }
func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// mergeOneKey folds all pending entries sharing e.key (e already taken as the
// first) against base, returning the merged loc and appending verdicts. present is
// true for a duplicate (base is the repository's loc) and false for a unique insert
// (base is the first pending loc). Last-writer-wins by lsn folds the index update
// into the dedup pass: a rediscovery whose record moved carries a newer lsn, and
// the merge takes the newer loc (section 3.4).
func mergeOneKey(ps *pendStream, e pendEntry, base locEntry, present bool, verdicts []Classification) (locEntry, []Classification) {
	loc := base
	if e.op == opCheckInsert {
		verdicts = append(verdicts, Classification{Key: e.key, Unique: !present})
	}
	if e.loc.lsn > loc.lsn {
		loc = e.loc
	}
	for ps.more() && ps.peek().key == e.key {
		d := ps.take()
		if d.op == opCheckInsert {
			verdicts = append(verdicts, Classification{Key: d.key, Unique: !present})
		}
		if d.loc.lsn > loc.lsn {
			loc = d.loc
		}
	}
	return loc, verdicts
}

// mergeCycle sweeps all pending runs against the sorted repository in one pass,
// classifying each pending key unique-or-duplicate and writing the merged
// repository to repository.next, then fsyncing and atomically renaming it over the
// live repository. It returns the dedup verdicts; the index update is the merged
// output itself, so dedup and index are one pass (section 3.4).
func mergeCycle(repoPath, nextPath, idxPath string, runs []*pendRun, readBuf, writeBuf int) ([]Classification, *blockIndex, error) {
	repo, err := openRepoReader(repoPath, readBuf)
	if err != nil {
		return nil, nil, err
	}
	defer repo.close()
	out, err := newRepoWriter(nextPath, repoPath, idxPath, writeBuf)
	if err != nil {
		return nil, nil, err
	}

	ps := newPendStream(runs)
	var verdicts []Classification

	rk, rok, err := repo.next()
	if err != nil {
		return nil, nil, err
	}
	for ps.more() {
		pk := ps.peek()
		// Carry repository keys below the next pending key through unchanged.
		for rok && rk.key.Less(pk.key) {
			if err := out.write(rk.key, rk.loc); err != nil {
				return nil, nil, err
			}
			if rk, rok, err = repo.next(); err != nil {
				return nil, nil, err
			}
		}

		if rok && rk.key == pk.key {
			// Duplicate: the key is already in the repository.
			e := ps.take()
			loc, v := mergeOneKey(ps, e, rk.loc, true, verdicts)
			verdicts = v
			if err := out.write(rk.key, loc); err != nil {
				return nil, nil, err
			}
			if rk, rok, err = repo.next(); err != nil {
				return nil, nil, err
			}
			continue
		}

		// Unique: the key is not in the repository.
		e := ps.take()
		loc, v := mergeOneKey(ps, e, e.loc, false, verdicts)
		verdicts = v
		if err := out.write(e.key, loc); err != nil {
			return nil, nil, err
		}
	}
	// Drain the repository tail: keys above every pending key, carried through.
	for rok {
		if err := out.write(rk.key, rk.loc); err != nil {
			return nil, nil, err
		}
		if rk, rok, err = repo.next(); err != nil {
			return nil, nil, err
		}
	}

	if err := out.finishAndSync(); err != nil {
		return nil, nil, err
	}
	if err := out.renameOver(); err != nil {
		return nil, nil, err
	}
	bi, err := loadBlockIndex(idxPath, repoPath)
	if err != nil {
		return nil, nil, err
	}
	return verdicts, bi, nil
}
