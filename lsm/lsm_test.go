package lsm

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func eventDone(ch <-chan struct{}) bool {
	select {
	case _, ok := <-ch:
		return !ok
	default:
		return false
	}
}

func fillTillEvent(lsm *LSM, event <-chan struct{}) {
	for i := 0; !eventDone(event); i++ {
		key := fmt.Sprintf("burst_%02d", i)
		lsm.Put(key, []byte("large_value_to_trigger_flush_logic_1234567890"))
	}
}

func TestLSM_PutGet(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	err = lsm.Put("key1", []byte("val1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, ok, err := lsm.Get("key1")
	if err != nil || !ok || !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Get failed: got %s, ok %v, err %v", string(val), ok, err)
	}
}

func TestLSM_Delete(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	lsm.Put("to_del", []byte("exists"))
	lsm.Delete("to_del")

	_, ok, _ := lsm.Get("to_del")
	if ok {
		t.Error("Key should be deleted (not found)")
	}
}

func TestLSM_Trigger_Flush(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	// take flush channel before fill
	flushDone := lsm.FlushDone()
	fillTillEvent(lsm, flushDone)

	lsm.RLock()
	hasSST := len(lsm.readers) > 0
	lsm.RUnlock()

	if !hasSST {
		t.Error("Expected SST after write")
	}
}

func TestLSM_PutGet_SST(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	err = lsm.Put("key1", []byte("val1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// take flush channel before fill
	flushDone := lsm.FlushDone()
	fillTillEvent(lsm, flushDone)

	if len(lsm.frozen) != 0 {
		t.Fatal("Frozen memtable must be empty")
	}

	if err := lsm.current.Reset(); err != nil {
		t.Fatalf("Reset current memtable: %v", err)
	}

	// key1 must be in SST
	val, ok, err := lsm.Get("key1")
	if err != nil || !ok || !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Get failed: got %s, ok %v, err %v", string(val), ok, err)
	}
}

func TestLSM_Recovery(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	lsm.Put("persist_key", []byte("persist_val"))

	// take flush channel before fill
	flushDone := lsm.FlushDone()
	fillTillEvent(lsm, flushDone)

	if err := lsm.current.Reset(); err != nil {
		t.Fatalf("Reset current memtable: %v", err)
	}

	// a single value in current wal
	lsm.Put("not_flushed_key", []byte("not_flushed_val"))

	if err := lsm.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	newLsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load new failed: %v", err)
	}

	defer newLsm.Close()

	val, ok, _ := newLsm.Get("persist_key")
	if !ok || !bytes.Equal(val, []byte("persist_val")) {
		t.Errorf("Recovered sst data mismatch: got %s, ok %v", string(val), ok)
	}

	val, ok, _ = newLsm.Get("not_flushed_key")
	if !ok || !bytes.Equal(val, []byte("not_flushed_val")) {
		t.Errorf("Recovered unflushed data mismatch: got %s, ok %v", string(val), ok)
	}
}

func TestLSM_Scan(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	// go to sst after flush
	lsm.Put("a1", []byte("1"))
	lsm.Put("a2", []byte("2"))

	// take flush channel before fill
	flushDone := lsm.FlushDone()
	fillTillEvent(lsm, flushDone)

	if err := lsm.current.Reset(); err != nil {
		t.Fatalf("Reset current memtable: %v", err)
	}

	// stay in current memtable
	lsm.Put("a3", []byte("3"))

	it := lsm.Scan("a1", "a3")

	expected := []string{"a1", "a2", "a3"}
	count := 0
	for ; it.Valid(); it.Next() {
		if it.Key() != expected[count] {
			t.Errorf("Expected key %s, got %s", expected[count], it.Key())
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("Expected %d keys, got %d", len(expected), count)
	}
}

func TestLSM_Compact(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	// take compact channel before compaction
	compactDone := lsm.CompactDone()

	sstCompactThreshold := lsm.config.SSTCompactThreshold
	// check1 -- the oldest sst
	// check2 - check{sstCompactThreshold - 1} -- stuck somewhere in the middle
	// check{sstCompactThreshold} -- the last sst
	for round := 1; round <= sstCompactThreshold; round++ {
		for i := round; i <= sstCompactThreshold; i++ {
			key := fmt.Sprintf("check%02d", i)
			lsm.Put(key, []byte(fmt.Sprintf("large_value_%02d", i)))
		}

		// take flush channel before fill
		flushDone := lsm.FlushDone()
		fillTillEvent(lsm, flushDone)

		// since event is postponed we need to drop exceeded keys from the current table
		if err := lsm.current.Reset(); err != nil {
			t.Fatalf("Reset current memtable: %v", err)
		}
	}

	select {
	case <-compactDone:
	case <-time.After(time.Second * 5):
		t.Fatal("Compact timeout")
	}

	if len(lsm.readers) != 1 {
		t.Fatal("Expect a single SST after compaction")
	}

	for i := 1; i <= sstCompactThreshold; i++ {
		expKey := fmt.Sprintf("check%02d", i)
		expVal := []byte(fmt.Sprintf("large_value_%02d", i))
		val, ok, _ := lsm.Get(expKey)
		if !ok || !bytes.Equal(val, expVal) {
			t.Errorf("Unexpected value for key %s %s", expKey, string(val))
		}
	}
}
