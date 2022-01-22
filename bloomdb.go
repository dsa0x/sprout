package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	Err_rate float64

	// the size of the bit vector
	Bit_width uint64

	cache map[string]interface{}

	bit_array []bool
}

func NewBloom(err_rate float64, bit_width uint64) *BloomFilter {
	return &BloomFilter{
		Err_rate:  err_rate,
		Bit_width: bit_width,
		bit_array: make([]bool, bit_width),
		cache:     make(map[string]interface{}),
	}
}

func (bf *BloomFilter) Add(key string, val interface{}) error {

	idx := bf.candidates(key)

	bf.bit_array[idx[0]] = true
	bf.bit_array[idx[1]] = true

	bf.cache[key] = val

	return nil
}

func (bf *BloomFilter) Find(key string) bool {
	indices := bf.candidates(key)

	for i := 0; i < len(indices); i++ {
		if bf.bit_array[indices[i]] {
			return true
		}
	}
	return false
}

func (bf *BloomFilter) candidates(key string) []uint64 {
	h1, h2 := getHashes(key, int64(bf.Bit_width))

	idx1 := getIndex(h1, bf.Bit_width)
	idx2 := getIndex(h2, bf.Bit_width)

	return []uint64{idx1, idx2}
}

func getHashes(key string, width int64) (uint64, uint64) {
	sum := murmur3.Sum64([]byte(key))
	idx := sum
	fingerprint := sum%255 + 1
	altIdx := idx ^ (fingerprint * 0x5bd1e995)
	return idx, altIdx
}

func getIndex(index, width uint64) uint64 {
	return index % width
}

// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
// fnvHash implements the Fowler–Noll–Vo hash function
func fnvHash(key string) (int64, error) {
	var offsetBasis, fnvPrime int64
	offsetBasis = math.MaxInt64
	fnvPrime = 0x100000001b3
	hash := offsetBasis

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(&key)
	if err != nil {
		return 0, err
	}

	for i := 0; i < buf.Len(); i++ {
		b, err := buf.ReadByte()
		if err != nil {
			return 0, err
		}
		hash = hash * fnvPrime
		hash = int64(uint8(hash>>8) ^ uint8(b))
	}

	return hash, nil

}

func main() {
	blm := NewBloom(0.5, 100)
	blm.Add("foo", "bar")
	blm.Add("baz", "qux")

	fmt.Println(blm.Find("foo"))
	fmt.Println(blm.Find("ese"))

}
