package lsm

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/MikhailProg/lsm-tree-db/internal/memtable"
	"github.com/MikhailProg/lsm-tree-db/internal/sst"
)

const (
	filenameFormat    = "%06d"
	walFilenameFormat = filenameFormat + ".wal"
	sstFilenameFormat = filenameFormat + ".sst"
)

type reqType byte

const (
	typeUnknown reqType = iota
	typeAdd
	typeDel
	typeRotate
)

type putReq struct {
	op    reqType
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
	fileIndex      atomic.Int32
	writeSeq       atomic.Int64
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
	if err := os.MkdirAll(config.DbDir, 0755); err != nil {
		return nil, err
	}

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
	// Check the current table under lock if the key is not there
	// create a snapshot Ref() from frozen memtables and ssts
	if r, ok := l.current.Get(key); ok {
		l.RUnlock()
		// tomstone
		if r == nil {
			return nil, false, nil
		}
		return r, true, nil
	}

	frozen := make([]*memtable.MemTable, len(l.frozen))
	copy(frozen, l.frozen)
	for _, f := range frozen {
		f.Ref()
	}

	readers := make([]*sst.Reader, len(l.readers))
	copy(readers, l.readers)
	for _, r := range readers {
		r.Ref()
	}
	l.RUnlock()

	defer func() {
		for _, f := range frozen {
			f.UnRef()
		}
		for _, r := range readers {
			r.UnRef()
		}
	}()

	// Lookup a key, nil value in memtables and ssts is a tombstone,
	//  if it's found the key is deleted so return it's not in lsm
	for i := len(frozen) - 1; i >= 0; i-- {
		if r, ok := frozen[i].Get(key); ok {
			// tombstone
			if r == nil {
				return nil, false, nil
			}
			return r, true, nil
		}
	}

	for i := len(readers) - 1; i >= 0; i-- {
		r, ok, err := readers[i].Get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			// tombstone
			if r == nil {
				return nil, false, nil
			}
			return r, true, nil
		}
	}

	return nil, false, nil
}

func (l *LSM) sendReq(op reqType, key string, val []byte) error {
	req := l.reqPool.Get().(*putReq)
	req.op, req.key, req.val = op, key, val

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

	req.op, req.key, req.val = typeUnknown, "", nil
	l.reqPool.Put(req)

	return err
}

func (l *LSM) Put(key string, val []byte) error {
	if val == nil {
		return fmt.Errorf("nil slice reserved for db tombstone")
	}
	return l.sendReq(typeAdd, key, val)
}

func (l *LSM) Delete(key string) error {
	return l.sendReq(typeDel, key, nil)
}

func (l *LSM) Rotate() error {
	return l.sendReq(typeRotate, "", nil)
}
