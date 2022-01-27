package bloom

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

	// The error rate of the bloom filter (P)
	err_rate float64

	// the number of items intended to be added to the bloom filter (n)
	capacity int

	// the size of the bit vector (M)
	bit_width int

	// persistent storage
	db Store

	// the number of items added to the bloom filter
	count int

	// the bit vector
	bit_array []bool

	// m is the number bits per slice(hashFn)
	m int

	// the length of seeds define the number of hashing functions we use
	seeds []int64
}

func NewBloom(err_rate float64, capacity int, database Store) *BloomFilter {
	if err_rate <= 0 || err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if capacity < 0 {
		panic("Capacity must be greater than 0")
	}

	// P
	err_rate /= 100.0

	// number of hash functions (k)
	numHashFn := int(math.Ceil(math.Log2(1.0 / err_rate)))

	// M
	bit_width := int(math.Ceil((float64(capacity) * math.Abs(math.Log(err_rate))) /
		(math.Pow(math.Ln2, 2))))

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

// Add adds the key to the bloom filter
func (bf *BloomFilter) Add(key string, val []byte) {

	indices := bf.candidates(key)

	if bf.Find(key) {
		return
	}

	if bf.count >= bf.capacity {
		log.Fatalf("BloomFilter has reached full capacity")
		return
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
func (bf *BloomFilter) Find(key string) bool {
	indices := bf.candidates(key)
	return arrEvery(indices, bf.bit_array)
}

// Get Gets the key from the underlying persistent store
func (bf *BloomFilter) Get(key []byte) []byte {
	if !bf.hasStore() {
		return nil
	}

	if !bf.Find(string(key)) {
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
	return bf.db != nil && bf.db.IsReady()
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
	for _, seed := range bf.seeds {
		hash := getHash(key, seed)
		idx := getBucketIndex(hash, uint64(bf.bit_width))
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

// Size returns the total capacity of the scalable bloom filter
func (bf *BloomFilter) Capacity() int {
	return bf.capacity
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
