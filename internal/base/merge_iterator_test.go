package base

import (
	"bytes"
	"testing"
)

type mockIterator struct {
	keys []string
	vals [][]byte
	idx  int
}

func (m *mockIterator) Seek(key string) {
	m.idx = 0
	for m.idx < len(m.keys) && m.keys[m.idx] < key {
		m.idx++
	}
}
func (m *mockIterator) Valid() bool   { return m.idx < len(m.keys) }
func (m *mockIterator) Next()         { m.idx++ }
func (m *mockIterator) Key() string   { return m.keys[m.idx] }
func (m *mockIterator) Value() []byte { return m.vals[m.idx] }
func (m *mockIterator) Err() error    { return nil }
func (m *mockIterator) Close() error  { return nil }

func TestMergeIterator_PriorityAndDeDup(t *testing.T) {
	// new
	it0 := &mockIterator{
		keys: []string{"b", "d"},
		vals: [][]byte{[]byte("b-new"), []byte("d-new")},
	}

	// old
	it1 := &mockIterator{
		keys: []string{"a", "b", "c"},
		vals: [][]byte{[]byte("a-old"), []byte("b-old"), []byte("c-old")},
	}

	iters := []Iterator[string, []byte]{it0, it1}

	mi := NewMergeIterator(iters)

	expected := []struct {
		k string
		v string
	}{
		{"a", "a-old"},
		{"b", "b-new"},
		{"c", "c-old"},
		{"d", "d-new"},
	}

	count := 0
	for ; mi.Valid(); mi.Next() {
		if count >= len(expected) {
			t.Errorf("Extra keys found")
			break
		}

		exp := expected[count]
		if mi.Key() != exp.k {
			t.Errorf("At index %d: expected key %s, got %s", count, exp.k, mi.Key())
		}
		if !bytes.Equal(mi.Value(), []byte(exp.v)) {
			t.Errorf("At index %d: expected value %s, got %s", count, exp.v, string(mi.Value()))
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("Expected %d keys, got %d", len(expected), count)
	}
}

func TestMergeIterator_Empty(t *testing.T) {
	it := &mockIterator{}
	mi := NewMergeIterator([]Iterator[string, []byte]{it})
	if mi.Valid() {
		t.Error("MergeIterator should be invalid for empty source")
	}
}
