package sst

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const (
	testHashNum    = 7
	testBitsPerKey = 10
)

func TestSST_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "00001.sst")

	// sorted as if it's skiplist
	testData := []struct {
		key string
		val []byte
		del bool
	}{
		{"apple", []byte("red"), false},
		{"banana", []byte("yellow"), false},
		{"cherry", nil, true}, // Tombstone
		{"date", []byte("brown"), false},
		{"elderberry", []byte("purple"), false},
	}

	fWrite, err := os.OpenFile(sstPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to create sst file: %v", err)
	}

	writer := NewWriter(fWrite, testHashNum, testBitsPerKey)
	for _, d := range testData {
		if d.del {
			err = writer.Add(d.key, nil)
		} else {
			err = writer.Add(d.key, d.val)
		}
		if err != nil {
			t.Fatalf("failed to add key %s: %v", d.key, err)
		}
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	writer.Close()

	fRead, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("failed to open sst file: %v", err)
	}
	defer fRead.Close()

	reader := NewReader(fRead)
	if err := reader.LoadMetadata(); err != nil {
		t.Fatalf("reader failed to parse sst: %v", err)
	}

	for _, d := range testData {
		t.Run("Key_"+d.key, func(t *testing.T) {
			val, found, err := reader.Get(d.key)
			if err != nil {
				t.Fatalf("Get error: %v", err)
			}
			if !found {
				t.Fatalf("key %s should be found", d.key)
			}

			if d.del {
				if val != nil {
					t.Error("expected nil for deleted key (tombstone)")
				}
			} else {
				if !bytes.Equal(val, d.val) {
					t.Errorf("expected %s, got %s", string(d.val), string(val))
				}
			}
		})
	}

	t.Run("AbsentKey", func(t *testing.T) {
		val, found, err := reader.Get("non_existent_fruit")
		if err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if found || val != nil {
			t.Error("found key that was never added")
		}
	})
}

func TestSST_InvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.sst")

	os.WriteFile(path, make([]byte, 22), 0644)

	f, _ := os.Open(path)
	defer f.Close()

	reader := NewReader(f)
	err := reader.LoadMetadata()
	if err == nil {
		t.Error("expected error for invalid magic, got nil")
	}
}

func BenchmarkSST_Get(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.sst")

	f, _ := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	writer := NewWriter(f, testHashNum, testBitsPerKey)
	for i := 0; i < 1000; i++ {
		writer.Add(fmt.Sprintf("key%04d", i), []byte("value"))
	}
	writer.Flush()
	writer.Close()

	f2, _ := os.Open(path)
	reader := NewReader(f2)
	reader.LoadMetadata()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Get("key0500")
	}
}
