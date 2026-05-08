package skiplist

import (
	"cmp"
	"fmt"
	"math/rand/v2"
)

type Node[K cmp.Ordered, V any] struct {
	key  K
	val  V
	next []*Node[K, V]
}

type SkipList[K cmp.Ordered, V any] struct {
	head         Node[K, V]
	maxLevel     int
	nodesCount   int
	kvNodesCount int
}

func New[K cmp.Ordered, V any](maxLevel int) *SkipList[K, V] {
	if maxLevel > 32 {
		maxLevel = 32
	}
	return &SkipList[K, V]{
		head:     Node[K, V]{next: make([]*Node[K, V], maxLevel)},
		maxLevel: maxLevel,
	}
}

func (l *SkipList[K, V]) MaxLevel() int {
	return l.maxLevel
}

func (l *SkipList[K, V]) Find(key K) (V, bool) {
	curr := &l.head

	for i := l.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].key < key {
			curr = curr.next[i]
		}
	}

	if curr.next[0] != nil && curr.next[0].key == key {
		return curr.next[0].val, true
	}

	var zero V
	return zero, false
}

func (l *SkipList[K, V]) genLevel() int {
	level := 1
	for rand.Float64() < 0.25 && level < l.maxLevel {
		level++
	}
	return level
}

func (l *SkipList[K, V]) Insert(key K, val V) bool {
	curr := &l.head

	var update [32]*Node[K, V]

	for i := l.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].key < key {
			curr = curr.next[i]
		}

		if curr.next[i] != nil && curr.next[i].key == key {
			curr.next[i].val = val
			return false
		}

		update[i] = curr
	}

	level := l.genLevel()

	node := &Node[K, V]{
		key:  key,
		val:  val,
		next: make([]*Node[K, V], level),
	}

	l.nodesCount += level
	l.kvNodesCount++

	for level := level - 1; level >= 0; level-- {
		node.next[level] = update[level].next[level]
		update[level].next[level] = node
	}

	return true
}

func (l *SkipList[K, V]) Remove(key K) bool {
	curr := &l.head
	del := 0

	for i := l.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].key < key {
			curr = curr.next[i]
		}

		if curr.next[i] != nil && curr.next[i].key == key {
			curr.next[i] = curr.next[i].next[i]
			del++
		}
	}

	if del > 0 {
		l.nodesCount -= del
		l.kvNodesCount--
	}

	return del > 0
}

func (l *SkipList[K, V]) Show() {
	fmt.Println("SkipList nodes", l.nodesCount, "kvnodes", l.kvNodesCount)
	for i := l.maxLevel - 1; i >= 0; i-- {
		fmt.Printf("%3d:", i)
		for node := l.head.next[i]; node != nil; node = node.next[i] {
			fmt.Print(" (", node.key, " ", node.val, ")")
		}
		fmt.Println()
	}
}
