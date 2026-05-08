package bloom

import (
	"fmt"
	"testing"
)

func TestBloomFilter_Basic(t *testing.T) {
	// 7 hashes, 100 keys, 10 bits per key
	f := NewCRC64(7, 100, 10)

	keys := []string{"apple", "banana", "orange", "go-lang", "lsm-tree"}
	for _, k := range keys {
		f.Add(k)
	}

	for _, k := range keys {
		if !f.Contains(k) {
			t.Errorf("filter should contain key: %s", k)
		}
	}

	missing := []string{"microsoft", "google", "rust", "database"}
	for _, k := range missing {
		if f.Contains(k) {
			t.Logf("info: false positive detected for key: %s", k)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := 1000
	bitsPerKey := 10
	hashNum := 7

	f := NewCRC64(hashNum, n, bitsPerKey)

	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("key_%d", i))
	}

	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		if f.Contains(fmt.Sprintf("other_%d", i)) {
			falsePositives++
		}
	}

	fpr := float64(falsePositives) / float64(testCount) * 100
	t.Logf("False Positive Rate (FPR) for %d bits/key: %.2f%%", bitsPerKey, fpr)

	if fpr > 5.0 {
		t.Errorf("FPR is too high: %.2f%%", fpr)
	}
}

func TestBloomFilter_DataPersistence(t *testing.T) {
	f1 := NewCRC64(7, 100, 10)
	f1.Add("persistent-key")

	data := f1.Data()

	f2 := NewCRC64FromData(data, 7, 100, 10)

	if !f2.Contains("persistent-key") {
		t.Error("restored filter lost data")
	}
}

func BenchmarkBloomFilter_Add(b *testing.B) {
	f := NewCRC64(7, b.N, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add("some_random_key")
	}
}

func BenchmarkBloomFilter_Contains(b *testing.B) {
	f := NewCRC64(7, 1000, 10)
	f.Add("search_key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Contains("search_key")
	}
}
