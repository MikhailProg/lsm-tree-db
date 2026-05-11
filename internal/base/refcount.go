package base

import (
	"sync"
	"sync/atomic"
)

type RefCount struct {
	mu      sync.Mutex // lock for destroy
	count   atomic.Int32
	release func() error
}

func (rc *RefCount) Init() {
	rc.count.Store(1)
}

func (rc *RefCount) Counter() int32 {
	return rc.count.Load()
}

func (rc *RefCount) Ref() {
	if rc.count.Add(1) <= 1 {
		panic("base.RefCount: Ref() called on a dead object")
	}
}

func (rc *RefCount) OnRelease(release func() error) {
	rc.mu.Lock()
	rc.release = release
	rc.mu.Unlock()
}

func (rc *RefCount) UnRef() (bool, error) {
	newCount := rc.count.Add(-1)
	if newCount == 0 {
		rc.mu.Lock()
		release := rc.release
		rc.mu.Unlock()
		if release != nil {
			if err := release(); err != nil {
				return false, err
			}
		}
		return true, nil
	}

	if newCount < 0 {
		panic("base.RefCount: UnRef called on object with zero reference count")
	}

	return false, nil
}
