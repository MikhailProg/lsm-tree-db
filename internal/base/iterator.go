package base

import "cmp"

type Iterator[K cmp.Ordered, V any] interface {
	Seek(K)
	Valid() bool
	Next()
	Key() K
	Value() V
	Err() error
	Close() error
}
