package lsm

import (
	"fmt"
	"os"
	"path/filepath"

	"github.io/MikhailProg/lsm-tree-db/internal/base"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
)

func compact(sstWriter *sst.Writer, ssts []*sst.Reader) error {
	iterators := make([]base.Iterator[string, []byte], len(ssts))
	for i := range ssts {
		iterators[i] = sst.NewIterator(ssts[i])
	}

	mi := base.NewMergeIterator(iterators)

	for ; mi.Valid(); mi.Next() {
		key, val := mi.Key(), mi.Value()
		if err := sstWriter.Add(key, val); err != nil {
			return fmt.Errorf("sst writer add key %s: %w", key, err)
		}

	}

	if mi.Err() != nil {
		return mi.Err()
	}

	return sstWriter.Flush()
}

func (l *LSM) genSSTFilename(fileIndex int) string {
	return filepath.Join(l.config.DbDir, fmt.Sprintf(sstFilenameFormat, fileIndex))
}

func (l *LSM) compactLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.compactWake:
		}

		l.RLock()
		compact := len(l.readers) >= l.config.SSTCompactThreshold
		l.RUnlock()

		if compact {
			if err := l.RunCompaction(); err != nil {
				panic(fmt.Errorf("critical error at compaction: %v", err))
			}
			l.Lock()
			{
				close(l.compactDone)
				l.compactDone = make(chan struct{})
			}
			l.Unlock()
		}
	}
}

func (l *LSM) wakeCompaction() {
	select {
	case l.compactWake <- struct{}{}:
	default:
	}
}

func (l *LSM) RunCompaction() error {
	var ssts []*sst.Reader

	fileIndex := 0
	l.Lock()
	{
		if len(l.readers) < 2 {
			l.Unlock()
			return nil
		}

		ssts = make([]*sst.Reader, 0, len(l.readers))
		for i := len(l.readers) - 1; i >= 0; i-- {
			ssts = append(ssts, l.readers[i])
		}
		l.fileIndex++
		fileIndex = l.fileIndex
	}
	l.Unlock()

	sstfilename := l.genSSTFilename(fileIndex)
	newsstfilename := sstfilename + ".new"

	sstFile, err := os.Create(newsstfilename)
	if err != nil {
		return fmt.Errorf("create %s: %w", newsstfilename, err)
	}

	w := sst.NewWriter(sstFile, l.config.HashNumber, l.config.BitsPerKey)

	if err := compact(w, ssts); err != nil {
		sstFile.Close()
		os.Remove(newsstfilename)
		return fmt.Errorf("compact to new sst %s: %w", newsstfilename, err)
	}

	// Close() method flush and close
	if err := w.Close(); err != nil {
		os.Remove(newsstfilename)
		return fmt.Errorf("close flushed sst %s: %w", newsstfilename, err)
	}

	if err := os.Rename(newsstfilename, sstfilename); err != nil {
		return fmt.Errorf("rename %s to %s", newsstfilename, sstfilename)
	}

	// Reopen for reading
	sstFile, err = os.Open(sstfilename)
	if err != nil {
		os.Remove(sstfilename)
		return fmt.Errorf("open file for reading %s: %w", sstfilename, err)
	}

	r := sst.NewReader(sstFile)
	if err := r.LoadMetadata(); err != nil {
		sstFile.Close()
		os.Remove(sstfilename)
		return fmt.Errorf("create reader: %w", err)
	}

	l.Lock()
	{
		for i := range len(ssts) {
			l.readers[i] = nil
		}
		l.readers = append([]*sst.Reader{r}, l.readers[len(ssts):]...)
	}
	l.Unlock()

	for i := range ssts {
		ssts[i].Close()
		os.Remove(ssts[i].Name())
	}

	return nil
}
