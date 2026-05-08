package lsm

import "fmt"

func (l *LSM) Close() error {
	l.cancel()
	l.wg.Wait()

	l.Lock()
	defer l.Unlock()

	var errs []error

	if err := l.current.Close(); err != nil {
		errs = append(
			errs, fmt.Errorf(
				"close current memtable %s: %w", l.current.Name(), err))
	}
	l.current = nil

	for _, table := range l.frozen {
		if err := table.Close(); err != nil {
			errs = append(
				errs, fmt.Errorf(
					"close frozen memtable %s: %w", table.Name(), err))
		}
	}
	l.frozen = nil

	for _, sst := range l.readers {
		if err := sst.Close(); err != nil {
			errs = append(
				errs, fmt.Errorf(
					"close sst reader %s: %w", sst.Name(), err))
		}
	}
	l.readers = nil

	if len(errs) > 0 {
		return fmt.Errorf("close lsm with errors: %v", errs)
	}

	return nil
}
