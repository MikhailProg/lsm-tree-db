package base

import (
	"testing"
)

func TestRefCount_RefUnrefDestroy(t *testing.T) {
	rc := RefCount{}

	count := 0

	rc.Init()
	rc.OnRelease(func() error {
		count++
		return nil
	})

	rc.Ref()
	if val := rc.Counter(); val != 2 {
		t.Errorf("Expect 2 but got %d", val)
	}

	ok, _ := rc.UnRef()
	if ok {
		t.Error("Object must not be released yet")
	}

	ok, _ = rc.UnRef()
	if !ok || count != 1 || rc.Counter() != 0 {
		t.Error("Object must be released")
	}
}
