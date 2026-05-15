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
	Seq uint64
	Key string
	Val []byte
}

type WAL struct {
	file *os.File
	wb   *bufio.Writer
	wbuf []byte
}

func WALOpenFile(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, fmt.Errorf("wal open file %s: %w", filename, err)
	}
	return file, err
}

func New(file *os.File) *WAL {
	return &WAL{file: file, wb: bufio.NewWriter(file)}
}

func (w *WAL) Name() string {
	return w.file.Name()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Write(op EntryType, seq uint64, key string, val []byte) error {
	// [CRC 4b][op 1b][seq 8b][keyLen 2b][valLen 4b][key][val]
	// calc crc over op, keyLen, valLen, key and val
	totalSize := 4 + 1 + 8 + 2 + 4 + len(key) + len(val)

	if totalSize > len(w.wbuf) {
		w.wbuf = make([]byte, totalSize)
	}

	buf := w.wbuf[:totalSize]

	buf[4] = byte(op)
	binary.LittleEndian.PutUint64(buf[5:13], seq)
	binary.LittleEndian.PutUint16(buf[13:15], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[15:19], uint32(len(val)))
	copy(buf[19:], key)
	copy(buf[19+len(key):], val)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	if _, err := w.wb.Write(buf); err != nil {
		return err
	}

	return nil
}

func (w *WAL) Sync() error {
	if err := w.wb.Flush(); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Reset() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}

	w.wb.Reset(w.file)
	return nil
}

func (w *WAL) Recover(fn func(WALEntry) error) error {
	// [CRC 4b][op 1b][seq 8b][keyLen 2b][valLen 4b][key][val]
	header := [19]byte{}
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
		seq := binary.LittleEndian.Uint64(header[5:13])
		keyLen := int(binary.LittleEndian.Uint16(header[13:15]))
		valLen := int(binary.LittleEndian.Uint32(header[15:19]))

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
			Seq: seq,
			Key: key,
			Val: valBuf,
		}

		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}
