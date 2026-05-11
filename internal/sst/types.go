package sst

type EntryType byte

const (
	EntryTypeAdd EntryType = 0
	EntryTypeDel EntryType = 1
)

const SSTMagic = "SSTB"

const (
	RecordHeaderSize = 7  // [type 1b][klen 2b][vlen 4b]
	FooterSize       = 30 // [indexOffset 8b][bloomOffset 8b][maxSeq 8b][hashNum 1b][bitsPerKey 1b][magic 4b]
)

type IndexRecord struct {
	key    string
	offset int64
	size   int32 // header + key + val
}
