package memtable

import (
	"os"
	"sync"

	"github.io/MikhailProg/lsm-tree-db/internal/base"
	"github.io/MikhailProg/lsm-tree-db/internal/skiplist"
	"github.io/MikhailProg/lsm-tree-db/internal/wal"
)

type MemTable struct {
	sync.RWMutex
	base.RefCount
	list   *skiplist.SkipList[string, []byte]
	wal    *wal.WAL
	size   int64
	maxSeq uint64
}

func New(file *os.File, maxLevel int) *MemTable {
	m := &MemTable{
		wal:  wal.New(file),
		list: skiplist.New[string, []byte](maxLevel),
	}

	m.Init()
	m.OnRelease(func() error {
		return m.Close()
	})

	return m
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
	m.size = 0
	return nil
}

func (m *MemTable) apply(op wal.EntryType, key string, val []byte) {
	oldVal, exist := m.list.Find(key)
	m.list.Insert(key, val)

	if op == wal.EntryTypeAdd {
		if !exist {
			m.size += int64(len(key) + len(val))
		} else {
			m.size += int64(len(val) - len(oldVal))
		}
	} else {
		if !exist {
			m.size += int64(len(key))
		} else {
			m.size -= int64(len(oldVal))
		}
	}
}

func (m *MemTable) Size() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.size
}

func (m *MemTable) MaxSeq() uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.maxSeq
}

func (m *MemTable) Put(seq uint64, key string, data []byte) error {
	m.Lock()
	defer m.Unlock()

	copyVal := make([]byte, len(data))

	copy(copyVal, data)

	err := m.wal.Write(wal.EntryTypeAdd, seq, key, copyVal)
	if err != nil {
		return err
	}

	m.maxSeq = seq
	m.apply(wal.EntryTypeAdd, key, copyVal)

	return nil
}

func (m *MemTable) Delete(seq uint64, key string) error {
	m.Lock()
	defer m.Unlock()

	err := m.wal.Write(wal.EntryTypeDel, seq, key, []byte{})
	if err != nil {
		return err
	}

	m.maxSeq = seq
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
		m.maxSeq = entry.Seq
		return nil
	})

	return err
}
