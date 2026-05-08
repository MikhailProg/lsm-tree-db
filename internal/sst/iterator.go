package sst

import "sort"

type Iterator struct {
	reader *Reader
	key    string
	idx    int
	err    error
}

func NewIterator(r *Reader) *Iterator {
	it := &Iterator{reader: r}
	it.updateKey()
	return it
}

func (it *Iterator) updateKey() {
	if it.Valid() {
		it.key = it.reader.index[it.idx].key
	} else {
		it.key = ""
	}
}

func (it *Iterator) Seek(key string) {
	it.idx = sort.Search(len(it.reader.index), func(i int) bool {
		return it.reader.index[i].key >= key
	})

	it.updateKey()
}

func (it *Iterator) Valid() bool {
	return it.err == nil && it.idx < len(it.reader.index)
}

func (it *Iterator) Next() {
	if it.Valid() {
		it.idx++
		it.updateKey()
	}
}

func (it *Iterator) Key() string {
	if !it.Valid() {
		return ""
	}
	return it.key
}

func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}

	_, val, err := it.reader.getFromIndex(it.idx)
	if err != nil {
		it.err = err
		return nil
	}

	return val
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	return nil
}
