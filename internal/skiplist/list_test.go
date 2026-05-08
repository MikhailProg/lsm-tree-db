package skiplist

import (
	"testing"
)

func TestSkipList_Basic(t *testing.T) {
	sl := New[int, string](16)

	sl.Insert(10, "ten")
	sl.Insert(20, "twenty")
	sl.Insert(5, "five")

	val, ok := sl.Find(10)
	if !ok || val != "ten" {
		t.Errorf("expected ten, got %v", val)
	}

	sl.Insert(10, "TEN_NEW")
	val, _ = sl.Find(10)
	if val != "TEN_NEW" {
		t.Errorf("expected TEN_NEW after update, got %v", val)
	}

	if !sl.Remove(20) {
		t.Error("expected true when removing existing key")
	}
	if _, ok := sl.Find(20); ok {
		t.Error("found key that should have been removed")
	}

	if sl.Remove(999) {
		t.Error("expected false when removing non-existent key")
	}
}

func TestSkipList_Iterator(t *testing.T) {
	sl := New[int, int](10)
	data := []int{3, 1, 4, 2, 5}
	for _, v := range data {
		sl.Insert(v, v*10)
	}

	it := NewIterator(sl)
	lastPriv := -1
	count := 0

	for ; it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		if k <= lastPriv {
			t.Errorf("iterator out of order: %d after %d", k, lastPriv)
		}
		if v != k*10 {
			t.Errorf("wrong value for key %d: %d", k, v)
		}
		lastPriv = k
		count++
	}

	if count != len(data) {
		t.Errorf("expected %d elements, iterated over %d", len(data), count)
	}
}

func TestSkipList_Iterator_Seek(t *testing.T) {
	sl := New[int, int](10)
	for i := 1; i <= 10; i++ {
		if i == 7 {
			continue
		}
		sl.Insert(i, i*10)
	}

	it := NewIterator(sl)

	it.Seek(5)
	if !it.Valid() || it.Key() != 5 {
		t.Errorf("expected to be at 5")
	}

	it.Seek(7)
	if !it.Valid() || it.Key() != 8 {
		t.Errorf("expected 8")
	}

	// jump back, Seek() starts from the head
	it.Seek(3)
	if !it.Valid() || it.Key() != 3 {
		t.Errorf("expected to jump back to 3, got %v", it.Key())
	}

	it.Seek(100)
	if it.Valid() {
		t.Error("iterator should be invalid after seeking past end")
	}
}

func TestSkipList_Large(t *testing.T) {
	sl := New[int, int](16)
	n := 1000

	for i := 0; i < n; i++ {
		sl.Insert(i, i)
	}

	for i := 0; i < n; i++ {
		if _, ok := sl.Find(i); !ok {
			t.Fatalf("could not find key %d", i)
		}
	}

	if sl.kvNodesCount != n {
		t.Errorf("wrong count: expected %d, got %d", n, sl.kvNodesCount)
	}
}

func BenchmarkSkipList_Insert(b *testing.B) {
	sl := New[int, int](16)
	for i := 0; i < b.N; i++ {
		sl.Insert(i, i)
	}
}

func BenchmarkSkipList_Find(b *testing.B) {
	sl := New[int, int](16)
	for i := 0; i < 1000; i++ {
		sl.Insert(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Find(i % 1000)
	}
}
