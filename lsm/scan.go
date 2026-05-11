package lsm

import (
	"github.io/MikhailProg/lsm-tree-db/internal/base"
	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
)

func (l *LSM) Scan(startKey, endKey string) (*RangeIterator, error) {
	// Wait till the current table becomes frozen
	if err := l.Rotate(); err != nil {
		return nil, err
	}

	l.Lock()
	// Make a snapshot, iterators call Ref() for each table
	iters := make(
		[]base.Iterator[string, []byte], 0, len(l.frozen)+len(l.readers))

	for i := len(l.frozen) - 1; i >= 0; i-- {
		iters = append(iters, memtable.NewIterator(l.frozen[i]))
	}

	for i := len(l.readers) - 1; i >= 0; i-- {
		iters = append(iters, sst.NewIterator(l.readers[i]))
	}

	l.Unlock()

	mi := base.NewMergeIterator(iters)

	return NewRangeIterator(mi, startKey, endKey), nil
}
