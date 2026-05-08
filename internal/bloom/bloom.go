package bloom

import (
	"hash/crc64"
	"unsafe"
)

var crcTable = crc64.MakeTable(crc64.ISO)

type BloomFilter interface {
	Add(key string)
	Contains(key string) bool
	Data() []byte
}

type CRC64 struct {
	data       []byte
	sizeBits   uint32
	hashNumber int
}

func NewCRC64(hashNumber, numOfKeys, bitsPerKey int) *CRC64 {
	if hashNumber < 2 {
		hashNumber = 2
	}

	return &CRC64{
		data:       make([]byte, (numOfKeys*bitsPerKey+7)/8),
		hashNumber: hashNumber,
		sizeBits:   uint32(numOfKeys * bitsPerKey),
	}
}

func NewCRC64FromData(data []byte, hashNumber, numOfKeys, bitsPerKey int) *CRC64 {
	if hashNumber < 2 {
		hashNumber = 2
	}

	return &CRC64{
		data:       data,
		hashNumber: hashNumber,
		sizeBits:   uint32(numOfKeys * bitsPerKey),
	}
}

func setBit(data []byte, nbit uint32) {
	data[nbit/8] |= 1 << (nbit % 8)
}

func checkBit(data []byte, nbit uint32) bool {
	return (data[nbit/8] & (1 << (nbit % 8))) > 0
}

func (f *CRC64) Add(key string) {
	sum := crc64.Checksum(
		unsafe.Slice(unsafe.StringData(key), len(key)),
		crcTable)
	h0 := uint32(sum)
	h1 := uint32(sum >> 32)

	for i := 0; i < f.hashNumber; i++ {
		h := (h0 + uint32(i)*h1) % f.sizeBits
		setBit(f.data, h)
	}
}

func (f *CRC64) Data() []byte {
	return f.data
}

func (f *CRC64) Contains(key string) bool {
	sum := crc64.Checksum(
		unsafe.Slice(unsafe.StringData(key), len(key)),
		crcTable)
	h0 := uint32(sum)
	h1 := uint32(sum >> 32)

	for i := 0; i < f.hashNumber; i++ {
		h := (h0 + uint32(i)*h1) % f.sizeBits
		if !checkBit(f.data, h) {
			return false
		}
	}

	return true
}
