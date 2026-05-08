package lsm

import (
	"github.io/MikhailProg/lsm-tree-db/internal/base"
	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
)

// It's not safe since compaction can close SSTs, need to add Ref() over sst.Reader
func (l *LSM) Scan(startKey, endKey string) *RangeIterator {
	l.RLock()
	defer l.RUnlock()

	var iters []base.Iterator[string, []byte]

	iters = append(iters, memtable.NewIterator(l.current))

	for i := len(l.frozen) - 1; i >= 0; i-- {
		iters = append(iters, memtable.NewIterator(l.frozen[i]))
	}

	for i := len(l.readers) - 1; i >= 0; i-- {
		iters = append(iters, sst.NewIterator(l.readers[i]))
	}

	mi := base.NewMergeIterator(iters)

	return NewRangeIterator(mi, startKey, endKey)
}
