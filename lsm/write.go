package lsm

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/MikhailProg/lsm-tree-db/internal/memtable"
	"github.com/MikhailProg/lsm-tree-db/internal/wal"
)

func (l *LSM) writeLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case req := <-l.writeQueue:
			l.handleReq(req)
		}
	}
}

func (l *LSM) handleReq(req *writeReq) {
	l.RLock()
	tableSize := l.current.Size()
	needRotate :=
		req.op == typeRotate && tableSize > 0 ||
			tableSize >= l.config.MaxMemTableSize
	l.RUnlock()

	if needRotate {
		// request a token from the semaphore
		select {
		case <-l.ctx.Done():
			return
		case <-l.flushSemFrozen:
		}

		if err := l.rotateMemTable(); err != nil {
			req.errCh <- err
			return
		}
		l.wakeFlusher()
	}

	var err error
	if req.op != typeRotate {
		seq := uint64(l.writeSeq.Add(1))
		l.Lock()
		switch req.op {
		case typeAdd:
			err = l.current.Put(seq, req.key, req.val)
		case typeDel:
			err = l.current.Delete(seq, req.key)
		}
		l.Unlock()
	}

	req.errCh <- err
}

func (l *LSM) genWALFilename(fileIndex int) string {
	return filepath.Join(l.config.DbDir, fmt.Sprintf(walFilenameFormat, fileIndex))
}

func (l *LSM) openWAL(fileIndex int) (*os.File, error) {
	walPath := l.genWALFilename(fileIndex)
	wal, err := wal.WALOpenFile(walPath)
	if err != nil {
		return nil, fmt.Errorf("create file %s: %w", walPath, err)
	}
	return wal, nil
}

func (l *LSM) rotateMemTable() error {
	walFile, err := l.openWAL(int(l.fileIndex.Add(1)))
	if err != nil {
		return err
	}

	l.Lock()
	defer l.Unlock()

	l.frozen = append(l.frozen, l.current)
	l.current = memtable.New(walFile, l.config.MaxMemTableLevel)

	return nil
}
