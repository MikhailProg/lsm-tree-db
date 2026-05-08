package lsm

import (
	"fmt"
	"os"
	"strings"

	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
)

func (l *LSM) flushLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.flushWake:
		}

		for l.flushFrozen() {
			select {
			case <-l.ctx.Done():
				return
				// Return token to queue
			case l.flushSemFrozen <- struct{}{}:
				// notify if someone is waiting for flushDone event
				// create new event for a new flush
				l.Lock()
				{
					close(l.flushDone)
					l.flushDone = make(chan struct{})
				}
				l.Unlock()
			}
			l.wakeCompaction()
		}
	}
}

func (l *LSM) flushFrozen() bool {
	var table *memtable.MemTable

	l.RLock()
	{
		if len(l.frozen) == 0 {
			l.RUnlock()
			return false
		}

		table = l.frozen[0]
	}
	l.RUnlock()

	r, err := l.flushToSST(table)
	if err != nil {
		panic(fmt.Errorf("critical error at flushing: %v", err))
	}

	// Pretend it's not crucial at table rotation
	_ = table.Close()
	_ = os.Remove(table.Name())

	l.Lock()
	{
		copy(l.frozen, l.frozen[1:])
		l.frozen[len(l.frozen)-1] = nil
		l.frozen = l.frozen[:len(l.frozen)-1]
		l.readers = append(l.readers, r)
	}
	l.Unlock()

	return true
}

func sst2wal(sst string) string {
	return strings.TrimSuffix(sst, ".sst") + ".wal"
}

func wal2sst(wal string) string {
	return strings.TrimSuffix(wal, ".wal") + ".sst"
}

func (l *LSM) flushToSST(table *memtable.MemTable) (*sst.Reader, error) {
	sstfilename := wal2sst(table.Name())
	newsstfilename := sstfilename + ".new"

	sstFile, err := os.Create(newsstfilename)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", newsstfilename, err)
	}

	w := sst.NewWriter(sstFile, l.config.HashNumber, l.config.BitsPerKey)

	for it := memtable.NewIterator(table); it.Valid(); it.Next() {
		key, val := it.Key(), it.Value()
		if err := w.Add(key, val); err != nil {
			w.Close()
			os.Remove(newsstfilename)
			return nil, err
		}
	}

	if err := w.Flush(); err != nil {
		os.Remove(newsstfilename)
		return nil, fmt.Errorf("flush sst %s: %w", newsstfilename, err)
	}

	if err := w.Close(); err != nil {
		os.Remove(newsstfilename)
		return nil, fmt.Errorf("close flushed sst %s: %w", newsstfilename, err)
	}

	if err := os.Rename(newsstfilename, sstfilename); err != nil {
		os.Remove(newsstfilename)
		return nil, fmt.Errorf("rename %s to %s", newsstfilename, sstfilename)
	}

	// Reopen for reading
	sstFile, err = os.Open(sstfilename)
	if err != nil {
		os.Remove(sstfilename)
		return nil, fmt.Errorf("open file for reading %s: %w", sstfilename, err)
	}

	r := sst.NewReader(sstFile)
	if err := r.LoadMetadata(); err != nil {
		sstFile.Close()
		os.Remove(sstfilename)
		return nil, fmt.Errorf("create reader: %w", err)
	}

	return r, nil
}

func (l *LSM) wakeFlusher() {
	select {
	case l.flushWake <- struct{}{}:
	default:
	}
}
