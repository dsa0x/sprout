package gobloomgo

import (
	"fmt"
	"log"
	"math"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

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

	memFile    *os.File
	mem        mmap.MMap
	pageOffset int
	lock       sync.Mutex

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

	pageOffset := 68

	// number of hash functions (k)
	numHashFn := int(math.Ceil(math.Log2(1.0 / err_rate)))

	//ln22 = ln2^2
	ln22 := math.Pow(math.Ln2, 2)

	// M
	bit_width := int((float64(capacity) * math.Abs(math.Log(err_rate)) / ln22))

	bit_width /= 2

	//m
	bits_per_slice := bit_width / numHashFn

	seeds := make([]int64, numHashFn)
	for i := 0; i < len(seeds); i++ {
		seeds[i] = int64((i + 1) << 16)
	}

	f, err := os.OpenFile("bloom.db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	if err := f.Truncate(int64(bit_width)); err != nil {
		log.Fatalf("Error truncating file: %s", err)
	}

	fmt.Println("bit_width:", bit_width, "numHashFn:", numHashFn)
	mem, err := mmap.MapRegion(f, bit_width, mmap.RDWR, 0, 0)
	if err != nil {
		log.Fatalf("Mmap error: %v", err)
	}

	_, err = f.WriteAt([]byte{0x0}, 0)
	if err != nil {
		log.Fatalf("Error writing to file: %v", err)
	}

	return &BloomFilter2{
		err_rate:   err_rate,
		capacity:   capacity,
		bit_width:  bit_width,
		memFile:    f,
		mem:        mem,
		pageOffset: pageOffset,
		m:          bits_per_slice,
		seeds:      seeds,
		db:         database,
		lock:       sync.Mutex{},
	}
}

// Add adds the key to the bloom filter
func (bf *BloomFilter2) Add(key, val []byte) {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error finding key:", r)
			os.Exit(1)
		}
	}()

	indices := bf.candidates(string(key))

	if bf.count >= bf.capacity {
		log.Panicf("BloomFilter has reached full capacity %d", bf.capacity)
	}

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndex(indices[i])
		bf.mem[idx] |= mask
	}
	bf.count++

	if bf.hasStore() {
		bf.db.Put([]byte(key), val)
	}

}

// Find checks if the key exists in the bloom filter
func (bf *BloomFilter2) Find(key []byte) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error finding key:", r)
			os.Exit(1)
		}
	}()

	indices := bf.candidates(string(key))
	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndex(indices[i])
		bit := bf.mem[idx]

		// check if the mask part of the bit is set
		if bit&mask == 0 {
			return false
		}
	}
	return true
}

// Get gets the key from the underlying persistent store
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

// getBitIndex returns the index and mask for the bit.
//
// The first half of the bits are set at the beginning of the byte,
// the second half at the end
func (bf *BloomFilter2) getBitIndex(idx uint64) (uint64, byte) {
	denom := uint64(bf.bit_width) / 2
	var mask byte
	if idx >= denom {
		mask = 0x0F // 00001111
		idx = idx % denom
	} else {
		mask = 0xF0 // 11110000
	}

	return idx, mask

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

// Close closes the file handle to the filter and the persistent store (if any)
func (bf *BloomFilter2) Close() error {
	if err := bf.mem.Flush(); err != nil {
		_ = bf.memFile.Close()
		return err
	}

	if err := bf.mem.Unmap(); err != nil {
		_ = bf.memFile.Close()
		return err
	}

	return bf.memFile.Close()
}

// Count returns the number of items added to the bloom filter
func (bf *BloomFilter2) Count() int {
	return bf.count
}

// FilterSize returns the size of the bloom filter
func (bf *BloomFilter2) FilterSize() int {
	return bf.bit_width
}
