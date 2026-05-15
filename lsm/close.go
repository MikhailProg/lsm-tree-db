package lsm

import "fmt"

func (l *LSM) Close() error {
	l.cancel()
	l.wg.Wait()

	l.Lock()
	defer l.Unlock()

	var errs []error

	if l.current.Size() > 0 {
		if _, err := l.doFlushToSST(l.current); err != nil {
			errs = append(errs, err)
		}
	}

	if _, err := l.current.UnRef(); err != nil {
		errs = append(
			errs, fmt.Errorf(
				"close current memtable %s: %w", l.current.Name(), err))
	}
	l.current = nil

	for i := len(l.frozen) - 1; i >= 0; i-- {
		table := l.frozen[i]
		if _, err := l.doFlushToSST(table); err != nil {
			errs = append(errs, err)
		}

		if _, err := table.UnRef(); err != nil {
			errs = append(
				errs, fmt.Errorf(
					"close frozen memtable %s: %w", table.Name(), err))
		}
	}
	l.frozen = nil

	for i := len(l.readers) - 1; i >= 0; i-- {
		sstReader := l.readers[i]
		if _, err := sstReader.UnRef(); err != nil {
			errs = append(
				errs, fmt.Errorf(
					"close sst reader %s: %w", sstReader.Name(), err))
		}
	}
	l.readers = nil

	if len(errs) > 0 {
		return fmt.Errorf("close lsm with errors: %v", errs)
	}

	return nil
}
