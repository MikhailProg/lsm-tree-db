package lsm

import (
	"fmt"
	"path/filepath"

	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/wal"
)

func (l *LSM) writeLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case req := <-l.writeQueue:
			l.RLock()
			rotate := l.current.GetTotalSize() >= l.config.MaxMemTableSize
			l.RUnlock()

			if rotate {
				// request a token from the semaphore
				select {
				case <-l.ctx.Done():
					return
				case <-l.flushSemFrozen:
				}

				if err := l.rotateMemTable(); err != nil {
					req.errCh <- err
					continue
				}
				l.wakeFlusher()
			}

			l.Lock()
			var err error
			if req.val != nil {
				err = l.current.Put(req.key, req.val)
			} else {
				err = l.current.Delete(req.key)
			}
			l.Unlock()

			req.errCh <- err
		}
	}
}

func (l *LSM) genWALFilename(fileIndex int) string {
	return filepath.Join(l.config.DbDir, fmt.Sprintf(walFilenameFormat, fileIndex))
}

func (l *LSM) rotateMemTable() error {
	walfilename := l.genWALFilename(l.fileIndex + 1)
	wal, err := wal.WALOpenFile(walfilename)
	if err != nil {
		return fmt.Errorf("create file %s: %w", walfilename, err)
	}

	l.Lock()
	{
		l.frozen = append(l.frozen, l.current)
		l.current = memtable.New(wal, l.config.MaxMemTableLevel)
		l.fileIndex++
	}
	l.Unlock()

	return nil
}
