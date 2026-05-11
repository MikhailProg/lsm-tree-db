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

	sstReader, err := l.flushToSST(table)
	if err != nil {
		panic(fmt.Errorf("critical error at flushing: %v", err))
	}

	l.Lock()
	{
		copy(l.frozen, l.frozen[1:])
		l.frozen[len(l.frozen)-1] = nil
		l.frozen = l.frozen[:len(l.frozen)-1]
		l.readers = append(l.readers, sstReader)
	}
	l.Unlock()

	table.OnRelease(func() error {
		if err := table.Close(); err != nil {
			return err
		}
		return os.Remove(table.Name())
	})
	// Pretend it's not crucial
	_, _ = table.UnRef()

	return true
}

func sst2wal(sst string) string {
	return strings.TrimSuffix(sst, ".sst") + ".wal"
}

func wal2sst(wal string) string {
	return strings.TrimSuffix(wal, ".wal") + ".sst"
}

func (l *LSM) doFlushToSST(table *memtable.MemTable) (string, error) {
	sstPath := wal2sst(table.Name())
	sstPathNew := sstPath + ".new"

	sstFile, err := os.Create(sstPathNew)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", sstPathNew, err)
	}

	sstWriter := sst.NewWriter(sstFile, table.MaxSeq(),
		l.config.HashNumber, l.config.BitsPerKey)

	sstWriterClean := func() {
		_ = sstWriter.Close()
		_ = os.Remove(sstWriter.Name())
	}

	it := memtable.NewIterator(table)

	for ; it.Valid(); it.Next() {
		key, val := it.Key(), it.Value()
		if err := sstWriter.Add(key, val); err != nil {
			sstWriterClean()
			return "", err
		}
	}

	if it.Err() != nil {
		sstWriterClean()
		return "", it.Err()
	}

	if err := sstWriter.Flush(); err != nil {
		sstWriterClean()
		return "", fmt.Errorf("flush sst %s: %w", sstWriter.Name(), err)
	}

	if err := sstWriter.Close(); err != nil {
		_ = os.Remove(sstWriter.Name())
		return "", fmt.Errorf("close flushed sst %s: %w", sstWriter.Name(), err)
	}

	if err := os.Rename(sstPathNew, sstPath); err != nil {
		_ = os.Remove(sstPathNew)
		return "", fmt.Errorf("rename %s to %s", sstPathNew, sstPath)
	}

	return sstPath, nil
}

func (l *LSM) flushToSST(table *memtable.MemTable) (*sst.Reader, error) {
	sstPath, err := l.doFlushToSST(table)
	if err != nil {
		return nil, err
	}

	sstFile, err := os.Open(sstPath)
	if err != nil {
		_ = os.Remove(sstPath)
		return nil, fmt.Errorf("open file for reading %s: %w", sstPath, err)
	}

	sstReader := sst.NewReader(sstFile)
	if err := sstReader.LoadMetadata(); err != nil {
		_, _ = sstReader.UnRef()
		_ = os.Remove(sstReader.Name())
		return nil, fmt.Errorf("load reader %s: %w", sstReader.Name(), err)
	}

	return sstReader, nil
}

func (l *LSM) wakeFlusher() {
	select {
	case l.flushWake <- struct{}{}:
	default:
	}
}
