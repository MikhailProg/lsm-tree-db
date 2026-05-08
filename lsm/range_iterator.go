package lsm

import (
	"github.io/MikhailProg/lsm-tree-db/internal/base"
)

type RangeIterator struct {
	*base.MergeIterator[string, []byte]
	endKey string
}

func NewRangeIterator(mi *base.MergeIterator[string, []byte], startKey string, endKey string) *RangeIterator {
	it := &RangeIterator{
		MergeIterator: mi,
		endKey:        endKey,
	}
	it.Seek(startKey)
	return it
}

func (it *RangeIterator) Valid() bool {
	for it.MergeIterator.Valid() && it.Value() == nil && it.Key() <= it.endKey {
		it.Next()
	}

	return it.MergeIterator.Valid() && it.Key() <= it.endKey
}

func (it *RangeIterator) Close() error {
	return it.MergeIterator.Close()
}
