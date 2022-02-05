package gobloomgo

import (
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/spaolacci/murmur3"
)

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

	memFile    *os.File
	mem        mmap.MMap
	pageOffset int
	lock       sync.Mutex
	byteSize   int

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

	f, err := os.OpenFile("bloom.db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	f.Truncate(0)

	var b byte
	byteSize := int(unsafe.Sizeof(&b))

	// we only need bit_width/8 bits, but only after calculating m
	bit_width /= byteSize
	bit_width += byteSize // add extra 1 byte to ensure we have a full byte at the end

	if err := f.Truncate(int64(bit_width)); err != nil {
		log.Fatalf("Error truncating file: %s", err)
	}

	mem, err := mmap.MapRegion(f, bit_width, mmap.RDWR, 0, 0)
	if err != nil {
		log.Fatalf("Mmap error: %v", err)
	}

	_, err = f.WriteAt([]byte{0x0}, 0)
	if err != nil {
		log.Fatalf("Error writing to file: %v", err)
	}

	return &BloomFilter{
		err_rate:  err_rate,
		capacity:  capacity,
		bit_width: bit_width,
		memFile:   f,
		mem:       mem,
		m:         bits_per_slice,
		seeds:     seeds,
		db:        database,
		lock:      sync.Mutex{},
		byteSize:  byteSize,
	}
}

// Add adds the key to the bloom filter
func (bf *BloomFilter) Add(key, val []byte) {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("Error adding key %s: %v", key, r)
			// os.Exit(1)
		}
	}()

	indices := bf.candidates(string(key))

	if bf.count >= bf.capacity {
		log.Panicf("BloomFilter has reached full capacity %d", bf.capacity)
	}

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])

		// set the bit at mask position of the byte at idx
		// e.g. if idx = 2 and mask = 01000000, set the bit at 2nd position of byte 2
		bf.mem[idx] |= mask
	}
	bf.count++

	if bf.hasStore() {
		bf.db.Put([]byte(key), val)
	}

}

// Find checks if the key exists in the bloom filter
func (bf *BloomFilter) Find(key []byte) bool {
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("Error finding key:", r)
			// os.Exit(1)
		}
	}()

	indices := bf.candidates(string(key))

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])
		bit := bf.mem[idx]

		// check if the mask part of the bit is set
		if bit&mask == 0 {
			return false
		}
	}
	return true
}

// Get gets the key from the underlying persistent store
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

// getBitIndex returns the index and mask for the bit. (unused)
//
// The first half of the bits are set at the beginning of the byte,
// the second half at the end
func (bf *BloomFilter) getBitIndex(idx uint64) (uint64, byte) {
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

// getBitIndexN returns the index and mask for the bit.
func (bf *BloomFilter) getBitIndexN(idx uint64) (uint64, byte) {
	quot, rem := divmod(int64(idx), int64(bf.byteSize))

	// shift the mask to the right by the remainder to get the bit index in the byte
	// if byteSize = 8,
	// 128 = 0x80 = 1000 0000, 128 >> 2 = 64.....and so on
	// 1000 0000 >> 2 = 0100 0000
	byteSizeInDec := int64(math.Pow(2, float64(bf.byteSize)-1))
	shift := byte((byteSizeInDec >> rem)) // 128 >> 1,2..

	return uint64(quot), shift
}

// candidates uses the hash function to get all index candidates of the given key
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

// Close closes the file handle to the filter and the persistent store (if any)
func (bf *BloomFilter) Close() error {
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
func (bf *BloomFilter) Count() int {
	return bf.count
}

// FilterSize returns the size of the bloom filter
func (bf *BloomFilter) FilterSize() int {
	return bf.bit_width
}

// divmod returns the quotient and remainder of a/b
func divmod(num, denom int64) (quot, rem int64) {
	quot = num / denom
	rem = num % denom
	return
}
