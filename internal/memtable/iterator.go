package memtable

import "github.io/MikhailProg/lsm-tree-db/internal/skiplist"

type Iterator struct {
	*skiplist.Iterator[string, []byte]
	table *MemTable
}

func NewIterator(table *MemTable) *Iterator {
	it := &Iterator{
		Iterator: skiplist.NewIterator(table.list),
		table:    table,
	}
	table.Ref()
	return it
}

func (it *Iterator) Close() error {
	_, err := it.table.UnRef()
	return err
}
