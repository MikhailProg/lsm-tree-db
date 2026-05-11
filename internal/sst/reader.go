package sst

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/MikhailProg/lsm-tree-db/internal/base"
	"github.com/MikhailProg/lsm-tree-db/internal/bloom"
)

type Reader struct {
	base.RefCount
	file   *os.File
	index  []IndexRecord
	bloom  *bloom.CRC64
	maxSeq uint64
}

func NewReader(file *os.File) *Reader {
	r := &Reader{file: file}
	r.Init()
	r.OnRelease(func() error {
		return r.Close()
	})
	return r
}

func (r *Reader) Name() string {
	return r.file.Name()
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) MaxSeq() uint64 {
	return r.maxSeq
}

func (r *Reader) LoadMetadata() error {
	info, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}

	footerOffset := info.Size() - FooterSize

	// [Data] -> [Index] -> [Bloom Filter] -> [Footer]
	var footer [FooterSize]byte
	if _, err := r.file.ReadAt(footer[:], footerOffset); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}

	if string(footer[26:]) != SSTMagic {
		return fmt.Errorf("invalid SST magic")
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[:8]))
	if _, err := r.file.Seek(indexOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seek to index: %w", err)
	}

	bloomOffset := int64(binary.LittleEndian.Uint64(footer[8:16]))
	maxSeq := binary.LittleEndian.Uint64(footer[16:24])
	hashNum := int(footer[24])
	bitsPerKey := int(footer[25])

	// Use buf reader to speedup reading index entries
	br := bufio.NewReader(r.file)

	index := []IndexRecord{}
	keyBuf := make([]byte, 4096)
	currOffset := indexOffset
	// Index info
	// [keyLen 2b][key][offset 8b][recordSize 4b]
	for currOffset < bloomOffset {
		var keyLenBuf [2]byte
		if _, err := io.ReadFull(br, keyLenBuf[:2]); err != nil {
			return err
		}

		keyLen := int(binary.LittleEndian.Uint16(keyLenBuf[:]))
		if keyLen > len(keyBuf) {
			keyBuf = make([]byte, keyLen)
		}
		if _, err := io.ReadFull(br, keyBuf[:keyLen]); err != nil {
			return err
		}

		key := string(keyBuf[:keyLen])

		var offsetBuf [8]byte
		if _, err := io.ReadFull(br, offsetBuf[:]); err != nil {
			return fmt.Errorf("read offset: %w", err)
		}
		offset := int64(binary.LittleEndian.Uint64(offsetBuf[:]))

		var sizeBuf [4]byte
		if _, err := io.ReadFull(br, sizeBuf[:]); err != nil {
			return fmt.Errorf("read record size: %w", err)
		}
		recordSize := int32(binary.LittleEndian.Uint32(sizeBuf[:]))

		index = append(index, IndexRecord{
			key:    key,
			offset: offset,
			size:   recordSize,
		})

		currOffset += int64(len(keyLenBuf) + keyLen + len(offsetBuf) + len(sizeBuf))
	}

	filterSize := footerOffset - bloomOffset
	filterBuf := make([]byte, filterSize)
	if _, err := r.file.ReadAt(filterBuf, bloomOffset); err != nil {
		return fmt.Errorf("read bloom filter: %w", err)
	}

	bloom := bloom.NewCRC64FromData(filterBuf, hashNum, len(index), bitsPerKey)

	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	r.bloom, r.index, r.maxSeq = bloom, index, maxSeq

	return nil
}

func (r *Reader) getFromIndex(n int) (string, []byte, error) {
	if n >= len(r.index) {
		return "", nil, fmt.Errorf("read out of index bound %d", n)
	}

	recordOffset := r.index[n].offset
	recordSize := r.index[n].size

	record := make([]byte, recordSize)
	if _, err := r.file.ReadAt(record, recordOffset); err != nil {
		return "", nil, fmt.Errorf("read record: %w", err)
	}

	header := record[:RecordHeaderSize]
	op := EntryType(header[0])
	keyLen := int(binary.LittleEndian.Uint16(header[1:3]))
	valLen := int(binary.LittleEndian.Uint32(header[3:7]))

	if len(record) != len(header)+keyLen+valLen {
		return "", nil, fmt.Errorf("corrupted record: size mismatch")
	}

	key := string(record[len(header) : len(header)+keyLen])

	if op == EntryTypeDel {
		return key, nil, nil
	}

	val := make([]byte, valLen)
	copy(val, record[len(header)+keyLen:])

	return key, val, nil
}

func (s *Reader) Get(key string) ([]byte, bool, error) {
	if ok := s.bloom.Contains(key); !ok {
		return nil, false, nil
	}

	n := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].key >= key
	})

	if n >= len(s.index) || s.index[n].key != key {
		return nil, false, nil
	}

	rkey, val, err := s.getFromIndex(n)
	if err != nil {
		return nil, false, err
	}

	if rkey != key {
		return nil, false, fmt.Errorf("key %s in record mismatch %s", key, rkey)
	}

	return val, true, nil
}
