package gobloomgo

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math"

	"github.com/spaolacci/murmur3"
)

var ErrKeyNotFound = fmt.Errorf("Key not found")

type BloomFilter struct {

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
func NewBloom(err_rate float64, capacity int, database Store) *BloomFilter {
	if err_rate <= 0 || err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if capacity <= 0 {
		panic("Capacity must be greater than 0")
	}

	// P = err_rate

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

	return &BloomFilter{
		err_rate:  err_rate,
		capacity:  capacity,
		bit_width: bit_width,
		bit_array: make([]bool, bit_width),
		m:         bits_per_slice,
		seeds:     seeds,
		db:        database,
	}
}

func NewBloomFromFile(path string) {

}

// Add adds the key to the bloom filter
func (bf *BloomFilter) Add(key, val []byte) {

	indices := bf.candidates(string(key))

	if bf.count >= bf.capacity {
		log.Panicf("BloomFilter has reached full capacity %d", bf.capacity)
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
func (bf *BloomFilter) Find(key []byte) bool {
	indices := bf.candidates(string(key))
	return arrEvery(indices, bf.bit_array)
}

// Get Gets the key from the underlying persistent store
func (bf *BloomFilter) Get(key []byte) []byte {
	if !bf.hasStore() {
		log.Panicf("BloomFilter has no persistent store. Use Find() instead")
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

func (bf *BloomFilter) hasStore() bool {
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
func (bf *BloomFilter) candidates(key string) []uint64 {
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

// getHash returns the non-cryptographic murmur hash of the key seeded with the given seed
func getHash(key string, seed int64) uint64 {
	hasher := murmur3.New64WithSeed(uint32(seed))
	hasher.Write([]byte(key))
	return hasher.Sum64()
}

// getBucketIndex returns the index of the bucket where the hash falls in
func getBucketIndex(hash, width uint64) uint64 {
	return hash % width
}

// Capacity returns the total capacity of the scalable bloom filter
func (bf *BloomFilter) Capacity() int {
	return bf.capacity
}

// Count returns the number of items added to the bloom filter
func (bf *BloomFilter) Count() int {
	return bf.count
}

// FilterSize returns the size of the bloom filter
func (bf *BloomFilter) FilterSize() int {
	return bf.bit_width
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
