package lsm

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
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

	it, err := lsm.Scan("a1", "a3")
	if err != nil {
		t.Fatalf("Create RangeIterator: %v", err)
	}

	expected := []string{"a1", "a2", "a3"}
	count := 0
	for ; it.Valid(); it.Next() {
		if it.Key() != expected[count] {
			t.Errorf("Expected key %s, got %s", expected[count], it.Key())
		}
		count++
	}

	if it.Err() != nil {
		t.Errorf("Range iterator error: %v", it.Err())
	}

	if count != len(expected) {
		t.Errorf("Expected %d keys, got %d", len(expected), count)
	}

	if err := it.Close(); err != nil {
		t.Errorf("Range iterator close: %v", it.Err())
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
	case <-time.After(time.Second * 10):
		t.Fatal("Compact timeout")
	}

	if len(lsm.readers) != 1 {
		t.Fatal("Expected a single SST after compaction")
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

func TestLSM_ScanRefUnref(t *testing.T) {
	tmpDir := t.TempDir()

	lsm, err := Open(DefaultConfig(tmpDir), context.Background())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	defer lsm.Close()

	var rangeIters []*RangeIterator
	var tables []*memtable.MemTable

	// take compact channel before compaction
	compactDone := lsm.CompactDone()
	sstCompactThreshold := lsm.config.SSTCompactThreshold

	// Iterate one before compaction treshold
	for i := 1; i <= sstCompactThreshold-1; i++ {
		key := fmt.Sprintf("a%d", i)
		lsm.Put(key, []byte(fmt.Sprintf("value%d", i)))

		current := lsm.current

		// take flush channel before Scan() rotate and flush table
		flushDone := lsm.FlushDone()
		ri, _ := lsm.Scan("a", "b")
		rangeIters = append(rangeIters, ri)

		select {
		case <-flushDone:
		case <-time.After(time.Second * 10):
			t.Fatal("Flush timeout")
		}

		if current == lsm.current {
			t.Error("Current table must be rotated")
		}

		tables = append(tables, current)
	}

	sstReaders := make([]*sst.Reader, len(lsm.readers))
	copy(sstReaders, lsm.readers)

	if len(sstReaders) != sstCompactThreshold-1 {
		t.Errorf("Expected %d SSTs, got %d", sstCompactThreshold-1, len(sstReaders))
	}

	// sst0 -- 1 from LSM + 1 from scan2 + ... + 1 from scanN
	// sst1 -- 1 from LSM + 1 from scan3 + ... + 1 from scanN
	// sstN -- 1 from LSM
	for i, sstReader := range sstReaders {
		if int(sstReader.Counter()) != len(sstReaders)-i {
			t.Errorf(
				"SST RefCount mismatch: expected %d (LSM + active scans), got %d",
				len(sstReaders)-i, sstReader.Counter())
		}
	}

	// take flush channel before fill
	flushDone := lsm.FlushDone()
	// fill current memtable to trigger flush than wait for compaction
	fillTillEvent(lsm, flushDone)

	select {
	case <-compactDone:
	case <-time.After(time.Second * 10):
		t.Fatal("Compact timeout")
	}

	// compaction must Unref() all ssts and create a new one
	if len(lsm.readers) != 1 {
		t.Fatal("Expected a single SST after compaction")
	}

	// sst0 -- 1 from scan2 + ... + 1 from scanN
	// sst1 -- 1 from scan3 + ... + 1 from scanN
	// sstN -- 0
	for i, sstReader := range sstReaders {
		if int(sstReader.Counter()) != len(sstReaders)-i-1 {
			t.Errorf(
				"SSTs should be held by iterator only: expected %d ref, got %d.",
				len(sstReaders)-i-1, sstReader.Counter())
		}
	}

	// tables must be still referenced by RangeIterators
	for _, table := range tables {
		if table.Counter() != 1 {
			t.Errorf(
				"MemTable should be held by iterator only: expected 1 ref, got %d.",
				table.Counter())
		}
	}

	for round := 1; round <= len(rangeIters); round++ {
		ri := rangeIters[round-1]
		for i := 1; i <= round; i++ {
			expKey := fmt.Sprintf("a%d", i)
			if ri.Key() != expKey {
				t.Errorf("Expected key %s, got %s", expKey, ri.Key())
			}
			expVal := []byte(fmt.Sprintf("value%d", i))
			if !bytes.Equal(ri.Value(), expVal) {
				t.Errorf("Expected val %s, got %s", string(expVal), string(ri.Value()))
			}
			ri.Next()
		}
		if ri.Valid() {
			t.Error("RangeIterator should be Invalud by the of loop")
		}
	}

	for _, ri := range rangeIters {
		if err := ri.Close(); err != nil {
			t.Fatalf("Close RangeIterator: %v", err)
		}
	}

	// all references must be gone for ssts and tables
	for _, sstReader := range sstReaders {
		if sstReader.Counter() != 0 {
			t.Errorf(
				"SST should be released: expected 0 ref, got %d.",
				sstReader.Counter())
		}
	}

	for _, table := range tables {
		if table.Counter() != 0 {
			t.Errorf(
				"MemTable should be released: expected 0 ref, got %d.",
				table.Counter())
		}
	}
}
