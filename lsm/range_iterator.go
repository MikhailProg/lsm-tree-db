package lsm

import (
	"github.io/MikhailProg/lsm-tree-db/internal/base"
)

type RangeIterator struct {
	mi     *base.MergeIterator[string, []byte]
	endKey string
}

func (it *RangeIterator) skipTombstone() {
	for it.Valid() && it.Value() == nil {
		it.mi.Next()
	}
}

func NewRangeIterator(mi *base.MergeIterator[string, []byte], startKey string, endKey string) *RangeIterator {
	it := &RangeIterator{
		mi:     mi,
		endKey: endKey,
	}
	it.Seek(startKey)
	return it
}

func (it *RangeIterator) Seek(key string) {
	it.mi.Seek(key)
	it.skipTombstone()
}

func (it *RangeIterator) Key() string {
	return it.mi.Key()
}

func (it *RangeIterator) Value() []byte {
	return it.mi.Value()
}

func (it *RangeIterator) Next() {
	it.mi.Next()
	it.skipTombstone()
}

func (it *RangeIterator) Valid() bool {
	return it.mi.Valid() && it.Key() <= it.endKey
}

func (it *RangeIterator) Close() error {
	return it.mi.Close()
}

func (it *RangeIterator) Err() error {
	return it.mi.Err()
}
