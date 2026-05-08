package base

import (
	"cmp"
	"container/heap"
)

type heapEntry[K cmp.Ordered] struct {
	key K
	ord int
}

type mergeHeap[K cmp.Ordered] []heapEntry[K]

type MergeIterator[K cmp.Ordered, V any] struct {
	iterators []Iterator[K, V]
	heap      mergeHeap[K]
	err       error
}

func (h mergeHeap[K]) Len() int {
	return len(h)
}

func (h mergeHeap[K]) Less(i, j int) bool {
	if h[i].key < h[j].key {
		return true
	}
	if h[i].key == h[j].key {
		return h[i].ord < h[j].ord
	}
	return false
}

func (h mergeHeap[K]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap[K]) Push(x any) {
	*h = append(*h, x.(heapEntry[K]))
}

func (h *mergeHeap[K]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func pushNext[K cmp.Ordered, V any](h *mergeHeap[K], it Iterator[K, V], ord int) error {
	if !it.Valid() {
		// if iter is done error will be nil
		return it.Err()
	}

	heap.Push(h, heapEntry[K]{
		key: it.Key(), ord: ord,
	})

	return nil
}

func NewMergeIterator[K cmp.Ordered, V any](iterators []Iterator[K, V]) *MergeIterator[K, V] {
	it := &MergeIterator[K, V]{iterators: iterators}

	for i, childIt := range it.iterators {
		if childIt.Err() != nil {
			it.err = childIt.Err()
			return it
		}
		if childIt.Valid() {
			it.heap = append(it.heap, heapEntry[K]{
				key: childIt.Key(), ord: i,
			})
		}
	}

	heap.Init(&it.heap)

	return it
}

func (it *MergeIterator[K, V]) Seek(key K) {
	it.heap = it.heap[:0]

	for i, childIt := range it.iterators {
		childIt.Seek(key)
		if childIt.Err() != nil {
			it.err = childIt.Err()
			return
		}
		if childIt.Valid() {
			it.heap = append(it.heap, heapEntry[K]{
				key: childIt.Key(), ord: i,
			})
		}
	}

	heap.Init(&it.heap)
}

func (it *MergeIterator[K, V]) Valid() bool {
	return it.Err() == nil && it.heap.Len() > 0
}

func (it *MergeIterator[K, V]) Next() {
	if !it.Valid() {
		return
	}

	key := it.heap[0].key

	for it.heap.Len() > 0 && it.heap[0].key == key {
		ent := heap.Pop(&it.heap).(heapEntry[K])
		childIt := it.iterators[ent.ord]
		childIt.Next()

		if err := pushNext(&it.heap, childIt, ent.ord); err != nil {
			it.err = err
			return
		}
	}
}

func (it *MergeIterator[K, V]) Key() K {
	var key K
	if it.Valid() {
		key = it.heap[0].key
	}
	return key
}

func (it *MergeIterator[K, V]) Value() V {
	var val V
	if it.Valid() {
		ord := it.heap[0].ord
		val = it.iterators[ord].Value()
	}
	return val
}

func (it *MergeIterator[K, V]) Err() error {
	return it.err
}

func (it *MergeIterator[K, V]) Close() error {
	var firstErr error
	for _, childIt := range it.iterators {
		if err := childIt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
