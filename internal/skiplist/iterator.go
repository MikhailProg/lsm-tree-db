package skiplist

import "cmp"

type Iterator[K cmp.Ordered, V any] struct {
	curr *Node[K, V]
	list *SkipList[K, V]
}

func NewIterator[K cmp.Ordered, V any](list *SkipList[K, V]) *Iterator[K, V] {
	return &Iterator[K, V]{
		curr: list.head.next[0],
		list: list,
	}
}

func (it *Iterator[K, V]) Seek(key K) {
	curr := &it.list.head
	for i := it.list.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].key < key {
			curr = curr.next[i]
		}
	}
	it.curr = curr.next[0]
}

func (it *Iterator[K, V]) Valid() bool {
	return it.curr != nil
}

func (it *Iterator[K, V]) Next() {
	if it.curr != nil {
		it.curr = it.curr.next[0]
	}
}

func (it *Iterator[K, V]) Key() K {
	return it.curr.key
}

func (it *Iterator[K, V]) Value() V {
	return it.curr.val
}

func (it *Iterator[K, V]) Err() error {
	return nil
}

func (it *Iterator[K, V]) Close() error {
	return nil
}
