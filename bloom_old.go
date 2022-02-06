package sprout

import (
	"fmt"
	"log"
	"math"
	"unsafe"
)

var ErrKeyNotFound = fmt.Errorf("Key not found")

type BloomFilter2 struct {

	// The desired false positive rate
	err_rate float64

	// the number of items intended to be added to the bloom filter (n)
	capacity int

	// the size of the bit vector (M)
	bit_width int

	// persistent storage
	db Store

	// the number of items added to the bloom filter
	count int

	// the bit array
	bit_array []uint8
	byteSize  int

	// m is the number bits per slice(hashFn)
	m int

	// one seed per hash function
	seeds []int64
}

// NewBloom2 creates a new bloom filter in-memory
// err_rate is the desired false positive rate. e.g. 0.1 error rate implies 1 in 1000
//
// capacity is the number of entries intended to be added to the filter
//
// database is the persistent store to attach to the filter. can be nil.
func NewBloom2(opts *BloomOptions) *BloomFilter2 {
	if opts.Err_rate <= 0 || opts.Err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if opts.Capacity <= 0 {
		panic("Capacity must be greater than 0")
	}

	// number of hash functions (k)
	numHashFn := int(math.Ceil(math.Log2(1.0 / opts.Err_rate)))

	//ln22 = ln2^2
	ln22 := math.Pow(math.Ln2, 2)

	// M
	bit_width := int((float64(opts.Capacity) * math.Abs(math.Log(opts.Err_rate)) / ln22))

	//m
	bits_per_slice := bit_width / numHashFn

	seeds := make([]int64, numHashFn)
	for i := 0; i < len(seeds); i++ {
		seeds[i] = int64((i + 1) << 16)
	}

	var b byte
	byteSize := int(unsafe.Sizeof(&b))

	// we only need bit_width/8 bits, but only after calculating m
	bit_width /= byteSize
	bit_width += byteSize // add extra 1 byte to ensure we have a full byte at the end

	return &BloomFilter2{
		err_rate:  opts.Err_rate,
		capacity:  opts.Capacity,
		bit_width: bit_width,
		bit_array: make([]uint8, bit_width),
		m:         bits_per_slice,
		seeds:     seeds,
		db:        opts.Database,
		byteSize:  byteSize,
	}
}

// Add adds the key to the bloom filter
func (bf *BloomFilter2) Add(key, val []byte) {

	indices := bf.candidates(string(key))

	if bf.count >= bf.capacity {
		log.Panicf("BloomFilter2 has reached full capacity %d, count: %d", bf.capacity, bf.count)
	}

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])
		bf.bit_array[idx] |= mask
	}
	bf.count++

	if bf.hasStore() {
		bf.db.Put([]byte(key), val)
	}

}

// Find checks if the key exists in the bloom filter
func (bf *BloomFilter2) Contains(key []byte) bool {
	indices := bf.candidates(string(key))

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])
		bit := bf.bit_array[idx]

		// check if the mask part of the bit is set
		if bit&mask == 0 {
			return false
		}
	}
	return true
}

// Get Gets the key from the underlying persistent store
func (bf *BloomFilter2) Get(key []byte) []byte {
	if !bf.hasStore() {
		log.Panicf("BloomFilter2 has no persistent store. Use Contains() instead")
	}

	if !bf.Contains(key) {
		return nil
	}

	val, err := bf.db.Get(key)
	if err != nil {
		fmt.Printf("Error getting key %s from db: %s\n", key, err)
		return nil
	}
	return val

}

func (bf *BloomFilter2) hasStore() bool {
	return bf.db != nil && bf.db.isReady()
}

// getBitIndexN returns the index and mask for the bit.
func (bf *BloomFilter2) getBitIndexN(idx uint64) (uint64, byte) {
	quot, rem := divmod(int64(idx), int64(bf.byteSize))

	byteSizeInDec := int64(math.Pow(2, float64(bf.byteSize)-1))
	shift := byte((byteSizeInDec >> rem)) // 128 >> 1,2..
	return uint64(quot), shift
}

// candidates uses the hash function to return all index candidates of the given key
func (bf *BloomFilter2) candidates(key string) []uint64 {
	res := make([]uint64, 0, len(bf.seeds))
	for i, seed := range bf.seeds {
		hash := getHash(key, seed)
		// each hash produces an index over m for its respective slice.
		// e.g. 0-140, 140-280, 280-420
		idx := uint64(i*bf.m) + getBucketIndex(hash, uint64(bf.m))
		res = append(res, idx)
	}
	return res
}

// Capacity returns the total capacity of the scalable bloom filter
func (bf *BloomFilter2) Capacity() int {
	return bf.capacity
}

// Count returns the number of items added to the bloom filter
func (bf *BloomFilter2) Count() int {
	return bf.count
}

// FilterSize returns the size of the bloom filter
func (bf *BloomFilter2) FilterSize() int {
	return bf.bit_width
}

// Close closes the file handle to the filter and the persistent store (if any)
func (bf *BloomFilter2) Close() error {
	if bf.hasStore() {
		return bf.db.Close()
	}
	return nil
}
