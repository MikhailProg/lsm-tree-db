package lsm

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
	"github.io/MikhailProg/lsm-tree-db/internal/wal"
)

func listFiles(dir, ext string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read db directory  %s: %w", dir, err)
	}

	var files []string
	for _, e := range ents {
		if e.Type().IsRegular() && filepath.Ext(e.Name()) == ext {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}

	return files, nil
}

func numberFromName(filename string) int {
	ext := filepath.Ext(filename)
	num, _ := strconv.Atoi(
		strings.TrimSuffix(filepath.Base(filename), ext))
	return num
}

func (l *LSM) recoverSSTFromWAL(sstPath string) (*sst.Reader, error) {
	walPath := sst2wal(sstPath)
	walFile, err := os.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf(
			"open wal %s to recover %s: %w", walPath, sstPath, err)
	}

	sstPathBad := sstPath + ".bad"
	if err := os.Rename(sstPath, sstPathBad); err != nil {
		return nil, fmt.Errorf(
			"rename sst %s to %s: %w", sstPath, sstPathBad, err)
	}

	table := memtable.New(walFile, l.config.MaxMemTableLevel)

	if err := table.Recover(); err != nil {
		_, _ = table.UnRef()
		return nil, fmt.Errorf("recover wal %s: %w", table.Name(), err)
	}

	sstReader, err := l.flushToSST(table)
	if err != nil {
		_, _ = table.UnRef()
		return nil, fmt.Errorf("flush recovered sst %s: %w", table.Name(), err)
	}

	if _, err := table.UnRef(); err != nil {
		return nil, fmt.Errorf("close wal after recovery %s: %w", table.Name(), err)
	}

	// Pretend it's not crucial
	_ = os.Remove(table.Name())
	_ = os.Remove(sstPathBad)

	return sstReader, nil
}

func (l *LSM) loadSSTs() error {
	sstPaths, err := listFiles(l.config.DbDir, ".sst")
	if err != nil {
		return err
	}

	slices.Sort(sstPaths)

	for _, sstPath := range sstPaths {
		sstFile, err := os.Open(sstPath)
		if err != nil {
			return fmt.Errorf("open sst %s: %w", sstPath, err)
		}

		sstReader := sst.NewReader(sstFile)

		if err := sstReader.LoadMetadata(); err != nil {
			ok, err := sstReader.UnRef()
			if !ok || err != nil {
				return fmt.Errorf("close sst %s: %s", sstReader.Name(), err)
			}

			sstReader, err = l.recoverSSTFromWAL(sstPath)
			if err != nil {
				return fmt.Errorf("recover sst from wal %s: %s", sstPath, err)
			}
		}

		l.readers = append(l.readers, sstReader)
	}

	return nil
}

func (l *LSM) loadWALs() error {
	walPaths, err := listFiles(l.config.DbDir, ".wal")
	if err != nil {
		return err
	}

	slices.Sort(walPaths)

	walPaths = slices.DeleteFunc(walPaths, func(walPath string) bool {
		sstPath := wal2sst(walPath)
		_, err := os.Stat(sstPath)
		return err == nil
	})

	for _, walPath := range walPaths {
		walFile, err := wal.WALOpenFile(walPath)
		if err != nil {
			return fmt.Errorf("open wal %s: %w", walPath, err)
		}

		table := memtable.New(walFile, l.config.MaxMemTableLevel)

		walPathBad := ""
		// WAL can be corrupted try to recover it as it is
		if err := table.Recover(); err != nil {
			walPathBad = table.Name() + ".bad"
		}

		r, err := l.flushToSST(table)
		if err != nil {
			_, _ = table.UnRef()
			return fmt.Errorf("flush wal to sst %s: %w", table.Name(), err)
		}

		l.readers = append(l.readers, r)

		// Pretend it is not crucial
		_, _ = table.UnRef()
		if len(walPathBad) > 0 {
			if err := os.Rename(table.Name(), walPathBad); err != nil {
				return fmt.Errorf(
					"rename %s to %s: %w", table.Name(), walPathBad, err)
			}
		} else {
			_ = os.Remove(table.Name())
		}
	}

	return nil
}

func (l *LSM) Load() error {
	if err := l.loadSSTs(); err != nil {
		return err
	}

	if err := l.loadWALs(); err != nil {
		return err
	}

	slices.SortFunc(l.readers, func(a, b *sst.Reader) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	maxSSTNum := -1
	if len(l.readers) > 0 {
		maxSSTNum = numberFromName(l.readers[len(l.readers)-1].Name())
	}

	l.fileIndex.Store(int32(maxSSTNum + 1))

	walFile, err := l.openWAL(int(l.fileIndex.Load()))
	if err != nil {
		return err
	}

	l.current = memtable.New(walFile, l.config.MaxMemTableLevel)

	return nil
}
