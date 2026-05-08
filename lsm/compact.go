package lsm

import (
	"fmt"
	"os"
	"path/filepath"

	"github.io/MikhailProg/lsm-tree-db/internal/base"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
)

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
			if err := l.compactOnce(); err != nil {
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

func mergeData(sstWriter *sst.Writer, sstReaders []*sst.Reader) error {
	iters := make([]base.Iterator[string, []byte], len(sstReaders))

	for i, r := range sstReaders {
		iters[i] = sst.NewIterator(r)
	}

	mi := base.NewMergeIterator(iters)

	for ; mi.Valid(); mi.Next() {
		key, val := mi.Key(), mi.Value()

		if err := sstWriter.Add(key, val); err != nil {
			return fmt.Errorf(
				"sst writer add key %s to %s: %w", key, sstWriter.Name(), err)
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

func (l *LSM) prepareCompaction() ([]*sst.Reader, int) {
	var sstReaders []*sst.Reader

	fileIndex := 0
	l.Lock()
	{
		if len(l.readers) < 2 {
			l.Unlock()
			return nil, 0
		}

		sstReaders = make([]*sst.Reader, 0, len(l.readers))
		for i := len(l.readers) - 1; i >= 0; i-- {
			sstReaders = append(sstReaders, l.readers[i])
		}

		l.fileIndex++
		fileIndex = l.fileIndex
	}
	l.Unlock()

	return sstReaders, fileIndex
}

func (l *LSM) doCompact(sstReaders []*sst.Reader, fileIndex int) (*sst.Reader, error) {
	sstPath := l.genSSTFilename(fileIndex)
	sstPathNew := sstPath + ".new"

	sstFile, err := os.Create(sstPathNew)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", sstPathNew, err)
	}

	w := sst.NewWriter(sstFile, l.config.HashNumber, l.config.BitsPerKey)

	if err := mergeData(w, sstReaders); err != nil {
		_ = w.Close()
		_ = os.Remove(w.Name())
		return nil, fmt.Errorf("compact to new sst %s: %w", w.Name(), err)
	}

	if err := w.Close(); err != nil {
		_ = os.Remove(w.Name())
		return nil, fmt.Errorf("close flushed sst %s: %w", w.Name(), err)
	}

	if err := os.Rename(sstPathNew, sstPath); err != nil {
		return nil, fmt.Errorf("rename %s to %s", sstPathNew, sstPath)
	}

	sstFile, err = os.Open(sstPath)
	if err != nil {
		_ = os.Remove(sstPath)
		return nil, fmt.Errorf("open file for reading %s: %w", sstPath, err)
	}

	r := sst.NewReader(sstFile)
	if err := r.LoadMetadata(); err != nil {
		_ = r.Close()
		_ = os.Remove(r.Name())
		return nil, fmt.Errorf("load data from sst reader %s: %w", r.Name(), err)
	}

	return r, nil
}

func (l *LSM) updateSSTReaders(r *sst.Reader, sstReaders []*sst.Reader) {
	l.Lock()
	{
		for i := range len(sstReaders) {
			l.readers[i] = nil
		}
		l.readers = append([]*sst.Reader{r}, l.readers[len(sstReaders):]...)
	}
	l.Unlock()
}

func (l *LSM) compactOnce() error {
	sstReaders, fileIndex := l.prepareCompaction()
	if sstReaders == nil {
		return nil
	}

	r, err := l.doCompact(sstReaders, fileIndex)
	if err != nil {
		return err
	}

	l.updateSSTReaders(r, sstReaders)

	for _, r := range sstReaders {
		_ = r.Close()
		_ = os.Remove(r.Name())
	}

	return nil
}
