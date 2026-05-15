package memtable

import (
	"path/filepath"
	"testing"

	"github.com/MikhailProg/lsm-tree-db/internal/wal"
)

const (
	testMaxLevel = 16
)

func TestMemTable_PutGet(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "0.wal")
	f, _ := wal.WALOpenFile(walPath)

	mt := NewWithWAL(wal.New(f), 0, testMaxLevel)

	seq := uint64(0)

	mt.Put(seq, "key1", []byte("value1"))
	val, ok := mt.Get("key1")
	if !ok || string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}

	mt.Put(seq, "key1", []byte("value2"))
	val, _ = mt.Get("key1")
	if string(val) != "value2" {
		t.Errorf("expected value2 after update, got %s", string(val))
	}
}

func TestMemTable_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	f, _ := wal.WALOpenFile(filepath.Join(tmpDir, "0.wal"))
	mt := NewWithWAL(wal.New(f), 0, testMaxLevel)

	seq := uint64(0)
	mt.Put(seq, "key1", []byte("value1"))
	mt.Delete(seq, "key1")

	val, found := mt.Get("key1")
	if !found {
		t.Error("expected key to be found as tombstone")
	}
	if val != nil {
		t.Error("expected nil value for deleted key")
	}
}

func TestMemTable_TotalSize(t *testing.T) {
	tmpDir := t.TempDir()
	f, _ := wal.WALOpenFile(filepath.Join(tmpDir, "0.wal"))
	mt := NewWithWAL(wal.New(f), 0, testMaxLevel)

	seq := uint64(0)
	mt.Put(seq, "k", []byte("v")) // 1+1=2
	if mt.Size() != 2 {
		t.Errorf("expected size 2, got %d", mt.Size())
	}

	mt.Put(seq, "k", []byte("vvv")) // 1+3=4
	if mt.Size() != 4 {
		t.Errorf("expected size 4, got %d", mt.Size())
	}

	mt.Delete(seq, "k") // 1 (key) + 0 (nil val) = 1
	if mt.Size() != 1 {
		t.Errorf("expected size 1, got %d", mt.Size())
	}
}

func TestMemTable_Recover(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "0.wal")

	seq := uint64(42)
	f1, _ := wal.WALOpenFile(walPath)
	mt1 := NewWithWAL(wal.New(f1), 0, testMaxLevel)
	mt1.Put(seq, "recover_me", []byte("data"))

	if err := mt1.Sync(); err != nil {
		t.Fatalf("failed to sync memtable: %v", err)
	}

	if err := mt1.Close(); err != nil {
		t.Fatalf("failed to close memtable: %v", err)
	}

	f2, _ := wal.WALOpenFile(walPath)
	mt2 := NewWithWAL(wal.New(f2), 1, testMaxLevel)
	err := mt2.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	val, ok := mt2.Get("recover_me")
	if !ok || string(val) != "data" {
		t.Errorf("recovered data mismatch: got %s, ok %v", string(val), ok)
	}

	if mt2.MaxSeq() != mt1.MaxSeq() {
		t.Errorf("recovered seq mismatch: expected %d, got %d", mt1.MaxSeq(), mt2.MaxSeq())
	}
}
