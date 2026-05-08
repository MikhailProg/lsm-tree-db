package lsm

import (
	"context"
	"fmt"
	"sync"

	"github.io/MikhailProg/lsm-tree-db/internal/memtable"
	"github.io/MikhailProg/lsm-tree-db/internal/sst"
)

const (
	filenameFormat    = "%06d"
	walFilenameFormat = filenameFormat + ".wal"
	sstFilenameFormat = filenameFormat + ".sst"
)

type putReq struct {
	key   string
	val   []byte
	errCh chan error
}

type Config struct {
	DbDir               string
	MaxMemTableSize     int64
	MaxMemTableLevel    int
	HashNumber          int
	BitsPerKey          int
	SSTCompactThreshold int
	SemFrozenMaxLen     int
}

type LSM struct {
	sync.RWMutex
	config         Config
	current        *memtable.MemTable
	frozen         []*memtable.MemTable
	readers        []*sst.Reader
	fileIndex      int
	writeQueue     chan *putReq
	flushSemFrozen chan struct{}
	flushDone      chan struct{}
	flushWake      chan struct{}
	compactDone    chan struct{}
	compactWake    chan struct{}
	startOnce      sync.Once
	ctx            context.Context
	cancel         context.CancelFunc
	reqPool        *sync.Pool
	wg             sync.WaitGroup
}

func DefaultConfig(dbDir string) Config {
	return Config{
		DbDir:               dbDir,
		MaxMemTableSize:     4096,
		MaxMemTableLevel:    16,
		HashNumber:          7,  // bloom filter
		BitsPerKey:          10, // bloom filter
		SSTCompactThreshold: 4,
		SemFrozenMaxLen:     8,
	}
}

func New(config Config, ctx context.Context) *LSM {
	ctx, cancel := context.WithCancel(ctx)

	lsm := &LSM{
		config:         config,
		flushSemFrozen: make(chan struct{}, config.SemFrozenMaxLen),
		flushWake:      make(chan struct{}, 1),
		flushDone:      make(chan struct{}),
		compactWake:    make(chan struct{}, 1),
		compactDone:    make(chan struct{}),
		writeQueue:     make(chan *putReq),
		ctx:            ctx,
		cancel:         cancel,
		wg:             sync.WaitGroup{},
		reqPool: &sync.Pool{
			New: func() any {
				return &putReq{errCh: make(chan error, 1)}
			},
		},
	}

	for range config.SemFrozenMaxLen {
		lsm.flushSemFrozen <- struct{}{}
	}

	return lsm
}

func Open(config Config, ctx context.Context) (*LSM, error) {
	lsm := New(config, ctx)
	if err := lsm.Load(); err != nil {
		return nil, err
	}
	lsm.Start()
	return lsm, nil
}

func (l *LSM) FlushDone() <-chan struct{} {
	l.RLock()
	defer l.RUnlock()
	return l.flushDone
}

func (l *LSM) CompactDone() <-chan struct{} {
	l.RLock()
	defer l.RUnlock()
	return l.compactDone
}

func (l *LSM) Start() {
	l.startOnce.Do(func() {
		l.wg.Add(3)

		go func() {
			defer l.wg.Done()
			l.flushLoop()
		}()

		go func() {
			defer l.wg.Done()
			l.writeLoop()
		}()

		go func() {
			defer l.wg.Done()
			l.compactLoop()
		}()
	})
}

func (l *LSM) Get(key string) ([]byte, bool, error) {
	l.RLock()
	defer l.RUnlock()

	if r, ok := l.current.Get(key); ok {
		if r == nil {
			return nil, false, nil
		}
		return r, true, nil
	}

	for i := len(l.frozen) - 1; i >= 0; i-- {
		if r, ok := l.frozen[i].Get(key); ok {
			if r == nil {
				return nil, false, nil
			}
			return r, true, nil
		}
	}

	for i := len(l.readers) - 1; i >= 0; i-- {
		r, ok, err := l.readers[i].Get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if r == nil {
				return nil, false, nil
			}
			return r, true, nil
		}
	}

	return nil, false, nil
}

func (l *LSM) sendReq(key string, val []byte) error {
	req := l.reqPool.Get().(*putReq)
	req.key, req.val = key, val

	select {
	case <-l.ctx.Done():
		return l.ctx.Err()
	case l.writeQueue <- req:
	}

	var err error
	select {
	case <-l.ctx.Done():
		err = l.ctx.Err()
	case err = <-req.errCh:
	}

	req.key, req.val = "", nil
	l.reqPool.Put(req)

	return err
}

func (l *LSM) Put(key string, val []byte) error {
	if val == nil {
		return fmt.Errorf("nil slice reserved for db tombstone")
	}
	return l.sendReq(key, val)
}

func (l *LSM) Delete(key string) error {
	return l.sendReq(key, nil)
}
