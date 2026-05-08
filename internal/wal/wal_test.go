package wal

import (
	"path/filepath"
	"testing"
)

func TestWAL_WriteAndRecover(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	f, err := WALOpenFile(walPath)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	w := New(f)

	entries := []WALEntry{
		{Op: EntryTypeAdd, Key: "user:1", Val: []byte("alice")},
		{Op: EntryTypeAdd, Key: "user:2", Val: []byte("bob")},
		{Op: EntryTypeDel, Key: "user:1", Val: []byte{}},
	}

	for _, e := range entries {
		if err := w.Write(e.Op, e.Key, e.Val); err != nil {
			t.Fatalf("failed to write entry: %v", err)
		}
	}
	f.Close()

	f2, err := WALOpenFile(walPath)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer f2.Close()

	w2 := New(f2)
	var recovered []WALEntry

	err = w2.Recover(func(e WALEntry) error {
		recovered = append(recovered, e)
		return nil
	})

	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(recovered))
	}

	for i := range entries {
		if recovered[i].Key != entries[i].Key || recovered[i].Op != entries[i].Op {
			t.Errorf("entry %d mismatch", i)
		}
	}
}

func TestWAL_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "empty.wal")

	f, _ := WALOpenFile(walPath)
	defer f.Close()

	w := New(f)
	count := 0
	err := w.Recover(func(e WALEntry) error {
		count++
		return nil
	})

	if err != nil {
		t.Errorf("recover empty file should not return error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries, got %d", count)
	}
}
