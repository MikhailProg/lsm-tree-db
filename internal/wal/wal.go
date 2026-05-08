package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type EntryType byte

const (
	EntryTypeAdd EntryType = iota
	EntryTypeDel
)

type WALEntry struct {
	Op  EntryType
	Key string
	Val []byte
}

type WAL struct {
	file *os.File
	wbuf []byte
}

func WALOpenFile(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open file %s: %w", filename, err)
	}
	return file, err
}

func New(file *os.File) *WAL {
	return &WAL{file: file}
}

func (w *WAL) Name() string {
	return w.file.Name()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Reset() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return w.file.Truncate(0)
}

func (w *WAL) Write(op EntryType, key string, val []byte) error {
	// [CRC 4b][op 1b][keyLen 2b][valLen 4b][key][val]
	// calc crc over op, keyLen, valLen, key and val
	totalSize := 4 + 1 + 2 + 4 + len(key) + len(val)

	if totalSize > len(w.wbuf) {
		w.wbuf = make([]byte, totalSize)
	}

	buf := w.wbuf[:totalSize]

	buf[4] = byte(op)
	binary.LittleEndian.PutUint16(buf[5:7], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[7:11], uint32(len(val)))
	copy(buf[11:], key)
	copy(buf[11+len(key):], val)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Recover(fn func(WALEntry) error) error {
	// [CRC 4b][op 1b][keyLen 2b][valLen 4b][key][val]
	header := [11]byte{}
	r := bufio.NewReader(w.file)

	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		storedCrc := binary.LittleEndian.Uint32(header[:4])
		op := EntryType(header[4])
		keyLen := int(binary.LittleEndian.Uint16(header[5:7]))
		valLen := int(binary.LittleEndian.Uint32(header[7:11]))

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return fmt.Errorf("truncated key: %w", err)
		}

		key := string(keyBuf)

		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return fmt.Errorf("truncated value for key %s: %w", key, err)
		}

		crc := crc32.NewIEEE()
		crc.Write(header[4:])
		crc.Write(keyBuf)
		crc.Write(valBuf)
		if storedCrc != crc.Sum32() {
			return fmt.Errorf("wal crc mismatch: log is corrupted")
		}

		entry := WALEntry{
			Op:  op,
			Key: key,
			Val: valBuf,
		}

		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}
