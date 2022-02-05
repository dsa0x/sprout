package sprout

import (
	"fmt"
	"log"
	"math"
	"os"

	"github.com/edsrzf/mmap-go"
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
	bit_array []bool
	memFile   *os.File
	mem       mmap.MMap

	// m is the number bits per slice(hashFn)
	m int

	// one seed per hash function
	seeds []int64
}

// NewBloom creates a new bloom filter.
// err_rate is the desired false positive rate. e.g. 0.1 error rate implies 1 in 1000
//
// capacity is the number of entries intended to be added to the filter
//
// database is the persistent store to attach to the filter. can be nil.
func NewBloom2(err_rate float64, capacity int, database Store) *BloomFilter2 {
	if err_rate <= 0 || err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if capacity <= 0 {
		panic("Capacity must be greater than 0")
	}

	// number of hash functions (k)
	numHashFn := int(math.Ceil(math.Log2(1.0 / err_rate)))

	//ln22 = ln2^2
	ln22 := math.Pow(math.Ln2, 2)

	// M
	bit_width := int((float64(capacity) * math.Abs(math.Log(err_rate)) / ln22))

	//m
	bits_per_slice := bit_width / numHashFn

	seeds := make([]int64, numHashFn)
	for i := 0; i < len(seeds); i++ {
		seeds[i] = int64((i + 1) << 16)
	}

	return &BloomFilter2{
		err_rate:  err_rate,
		capacity:  capacity,
		bit_width: bit_width,
		bit_array: make([]bool, bit_width),
		m:         bits_per_slice,
		seeds:     seeds,
		db:        database,
	}
}

// Add adds the key to the bloom filter
func (bf *BloomFilter2) Add(key, val []byte) {

	indices := bf.candidates(string(key))

	if bf.count >= bf.capacity {
		log.Panicf("BloomFilter2 has reached full capacity %d, count: %d", bf.capacity, bf.count)
	}

	for i := 0; i < len(indices); i++ {
		bf.bit_array[indices[i]] = true
	}
	bf.count++

	if bf.hasStore() {
		bf.db.Put([]byte(key), val)
	}

}

// Find checks if the key exists in the bloom filter
func (bf *BloomFilter2) Find(key []byte) bool {
	indices := bf.candidates(string(key))
	return arrEvery(indices, bf.bit_array)
}

// Get Gets the key from the underlying persistent store
func (bf *BloomFilter2) Get(key []byte) []byte {
	if !bf.hasStore() {
		log.Panicf("BloomFilter2 has no persistent store. Use Find() instead")
	}

	if !bf.Find(key) {
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

// every checks if each index in the indices array has a value of 1 in the bit array
func arrEvery(indices []uint64, bits []bool) bool {
	allExists := true
	for _, idx := range indices {
		if !bits[idx] {
			allExists = false
			return allExists
		}
	}
	return allExists
}

// candidates uses the hash function to return all index candidates of the given key
func (bf *BloomFilter2) candidates(key string) []uint64 {
	var res []uint64
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
