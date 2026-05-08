package memtable

import (
	"os"
	"sync"

	"github.io/MikhailProg/lsm-tree-db/internal/skiplist"
	"github.io/MikhailProg/lsm-tree-db/internal/wal"
)

type MemTable struct {
	sync.RWMutex
	list      *skiplist.SkipList[string, []byte]
	wal       *wal.WAL
	totalSize int64
}

type Iterator struct {
	*skiplist.Iterator[string, []byte]
}

func New(file *os.File, maxLevel int) *MemTable {
	return &MemTable{
		wal:  wal.New(file),
		list: skiplist.New[string, []byte](maxLevel),
	}
}

func NewIterator(m *MemTable) *Iterator {
	return &Iterator{Iterator: skiplist.NewIterator(m.list)}
}

func (m *MemTable) Name() string {
	return m.wal.Name()
}

func (m *MemTable) Close() error {
	return m.wal.Close()
}

func (m *MemTable) Reset() error {
	m.Lock()
	defer m.Unlock()

	if err := m.wal.Reset(); err != nil {
		return err
	}

	m.list = skiplist.New[string, []byte](m.list.MaxLevel())
	m.totalSize = 0
	return nil
}

func (m *MemTable) apply(op wal.EntryType, key string, val []byte) {
	oldVal, exist := m.list.Find(key)
	m.list.Insert(key, val)

	if op == wal.EntryTypeAdd {
		if !exist {
			m.totalSize += int64(len(key) + len(val))
		} else {
			m.totalSize += int64(len(val) - len(oldVal))
		}
	} else {
		if !exist {
			m.totalSize += int64(len(key))
		} else {
			m.totalSize -= int64(len(oldVal))
		}
	}
}

func (m *MemTable) GetTotalSize() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.totalSize
}

func (m *MemTable) Put(key string, data []byte) error {
	m.Lock()
	defer m.Unlock()

	copyVal := make([]byte, len(data))

	copy(copyVal, data)

	err := m.wal.Write(wal.EntryTypeAdd, key, copyVal)
	if err != nil {
		return err
	}

	m.apply(wal.EntryTypeAdd, key, copyVal)

	return nil
}

func (m *MemTable) Delete(key string) error {
	m.Lock()
	defer m.Unlock()

	err := m.wal.Write(wal.EntryTypeDel, key, []byte{})
	if err != nil {
		return err
	}

	m.apply(wal.EntryTypeDel, key, nil)

	return nil
}

func (m *MemTable) Get(key string) ([]byte, bool) {
	m.RLock()
	defer m.RUnlock()
	// Return nil as true for lsm to mark key as deleted
	return m.list.Find(key)
}

func (m *MemTable) Recover() error {
	err := m.wal.Recover(func(entry wal.WALEntry) error {
		switch entry.Op {
		case wal.EntryTypeAdd:
			m.apply(entry.Op, entry.Key, entry.Val)
		case wal.EntryTypeDel:
			m.apply(entry.Op, entry.Key, nil)
		}
		return nil
	})

	return err
}
