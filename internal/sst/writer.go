package sst

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/MikhailProg/lsm-tree-db/internal/bloom"
)

type Writer struct {
	*bufio.Writer
	file       *os.File
	offset     int64
	index      []IndexRecord
	maxSeq     uint64
	hashNumber int
	bitsPerKey int
}

func NewWriter(file *os.File, maxSeq uint64, hashNumber int, bitsPerKey int) *Writer {
	return &Writer{
		Writer:     bufio.NewWriter(file),
		file:       file,
		maxSeq:     maxSeq,
		hashNumber: hashNumber,
		bitsPerKey: bitsPerKey,
	}
}

func (w *Writer) Name() string {
	return w.file.Name()
}

func (w *Writer) Add(key string, val []byte) error {
	var header [RecordHeaderSize]byte

	if val != nil {
		header[0] = byte(EntryTypeAdd)
	} else {
		header[0] = byte(EntryTypeDel)
	}

	binary.LittleEndian.PutUint16(header[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(header[3:7], uint32(len(val)))

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.WriteString(key); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	if _, err := w.Write(val); err != nil {
		return fmt.Errorf("write val: %w", err)
	}

	recordSize := int32(len(header) + len(key) + len(val))

	w.index = append(w.index, IndexRecord{
		key: key, offset: w.offset, size: recordSize})
	w.offset += int64(len(header) + len(key) + len(val))

	return nil
}

func (w *Writer) flushIndex() (*bloom.CRC64, error) {
	filter := bloom.NewCRC64(w.hashNumber, len(w.index), w.bitsPerKey)

	// [keyLen 2b][key][offset 8b][size 4b]
	for i := range w.index {
		key, offset, size :=
			w.index[i].key, w.index[i].offset, w.index[i].size

		filter.Add(key)

		var keyLenBuf [2]byte
		binary.LittleEndian.PutUint16(keyLenBuf[:], uint16(len(key)))
		if _, err := w.Write(keyLenBuf[:]); err != nil {
			return nil, fmt.Errorf("write index key ley: %w", err)
		}
		if _, err := w.WriteString(key); err != nil {
			return nil, fmt.Errorf("write index key: %w", err)
		}
		var offsetBuf [8]byte
		binary.LittleEndian.PutUint64(offsetBuf[:], uint64(offset))
		if _, err := w.Write(offsetBuf[:]); err != nil {
			return nil, fmt.Errorf("write index offset: %w", err)
		}
		var sizeBuf [4]byte
		binary.LittleEndian.PutUint32(sizeBuf[:], uint32(size))
		if _, err := w.Write(sizeBuf[:]); err != nil {
			return nil, fmt.Errorf("write index record size: %w", err)
		}
		w.offset += int64(len(keyLenBuf) + len(key) + len(offsetBuf) + len(sizeBuf))
	}

	return filter, nil
}

func (w *Writer) Flush() error {
	indexOffset := w.offset

	// Index
	bloom, err := w.flushIndex()
	if err != nil {
		return err
	}

	bloomOffset := w.offset
	// Bloom filter
	if _, err := w.Write(bloom.Data()); err != nil {
		return fmt.Errorf("write bloom filter: %w", err)
	}

	// Footer
	var footer [FooterSize]byte
	binary.LittleEndian.PutUint64(footer[:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[16:24], w.maxSeq)
	footer[24] = byte(w.hashNumber)
	footer[25] = byte(w.bitsPerKey)
	copy(footer[26:], []byte(SSTMagic))
	if _, err := w.Write(footer[:]); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}

	return w.Writer.Flush()
}

func (w *Writer) Close() error {
	if err := w.Writer.Flush(); err != nil {
		w.file.Close()
		return err
	}

	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return err
	}

	return w.file.Close()
}
