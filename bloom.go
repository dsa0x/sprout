package sprout

import (
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"unsafe"

	"github.com/dsa0x/sprout/pkg/murmur"
	"github.com/edsrzf/mmap-go"
	"github.com/juju/fslock"
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
	flock      *fslock.Lock
	byteSize   int

	// m is the number bits per slice(hashFn)
	m int

	// k is the number of hash functions
	k int

	// one seed per hash function
	seeds []int64

	path string
	opts *BloomOptions
}

// BloomOptions is the options for creating a new bloom filter
type BloomOptions struct {

	// path to the filter
	Path string

	// The desired false positive rate
	Err_rate float64

	// the number of items intended to be added to the bloom filter (n)
	Capacity int

	// persistent storage
	Database Store

	// growth rate of the bloom filter (valid values are 2 and 4)
	GrowthRate GrowthRate

	dataSize int
}

var DefaultBloomOptions = BloomOptions{
	Path:       "bloom.db",
	Err_rate:   0.001,
	Capacity:   10000,
	GrowthRate: 2,
	Database:   nil,
}

// NewBloom creates a new bloom filter.
// err_rate is the desired false error rate. e.g. 0.001 implies 1 false positive in 1000 lookups
//
// capacity is the number of entries intended to be added to the filter
//
// database is the persistent store to attach to the filter. can be nil.
func NewBloom(opts *BloomOptions) *BloomFilter {
	if opts == nil {
		opts = &DefaultBloomOptions
	}
	if opts.Err_rate <= 0 || opts.Err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if opts.Capacity <= 10 {
		panic("Capacity must be greater than 10")
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
		seeds[i] = 64 << int64((i + 1))
	}

	if opts.Path == "" {
		opts.Path = "/tmp/bloom.db"
	}

	var b byte
	byteSize := int(unsafe.Sizeof(&b))

	// we only need bit_width/8 bits, but only after calculating m
	bit_width /= byteSize
	bit_width += byteSize // add extra 1 byte to ensure we have a full byte at the end

	pageOffset := opts.dataSize
	opts.dataSize += bit_width // will be the offset of the next filter

	bf := &BloomFilter{
		err_rate:   opts.Err_rate,
		capacity:   opts.Capacity,
		bit_width:  bit_width,
		m:          bits_per_slice,
		seeds:      seeds,
		db:         opts.Database,
		lock:       sync.Mutex{},
		byteSize:   byteSize,
		path:       opts.Path,
		k:          numHashFn,
		pageOffset: pageOffset,
		opts:       opts,
	}

	// initialize advisory lock
	bf.flock = fslock.New(bf.path)

	// open the file
	err := bf.openFile()
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}

	// open mmap the file
	err = bf.mmap()
	if err != nil {
		log.Fatalf("Mmap error: %v", err)
	}

	return bf
}

// Add adds the key to the bloom filter
func (bf *BloomFilter) Add(key []byte) error {
	bf.lock.Lock()
	defer bf.lock.Unlock()

	indices := bf.candidates(string(key))

	if bf.count >= bf.capacity {
		return fmt.Errorf("BloomFilter has reached full capacity %d", bf.capacity)
	}

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])

		if int(idx) >= len(bf.mem) {
			return fmt.Errorf("Error finding key: Index out of bounds")
		}

		// set the bit at mask position of the byte at idx
		// e.g. if idx = 2 and mask = 01000000, set the bit at 2nd position of byte 2
		bf.mem[idx] |= mask
	}
	bf.count++
	return nil
}

// Put adds the key to the bloom filter, and also stores it in the persistent store
func (bf *BloomFilter) Put(key, val []byte) error {
	if !bf.hasStore() {
		return fmt.Errorf("BloomFilter does not have a store, use Add() to add keys")
	}

	bf.Add(key)
	return bf.db.Put([]byte(key), val)
}

// Contains checks if the key exists in the bloom filter
func (bf *BloomFilter) Contains(key []byte) bool {
	indices := bf.candidates(string(key))

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])

		if int(idx) >= len(bf.mem) {
			return false
		}
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
		log.Panicf("BloomFilter has no persistent store. Use Contains() instead")
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

// Merge merges the filter with another bloom filter.
// Both filters must have the same capacity and error rate.
// merging increases the false positive rate of the resulting filter
func (bf *BloomFilter) Merge(bf2 *BloomFilter) error {
	if bf.k != bf2.k {
		return fmt.Errorf("BloomFilter k values do not match")
	}
	if bf.bit_width != bf2.bit_width {
		return fmt.Errorf("BloomFilter bit_width values do not match")
	}

	bf.lock.Lock()
	defer bf.lock.Unlock()

	bf2.lock.Lock()
	defer bf2.lock.Unlock()

	for i := 0; i < bf.bit_width; i++ {
		bf.mem[i] |= bf2.mem[i]
	}

	return nil
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

// getHash returns the non-cryptographic murmur hash of the key seeded with the given seed
func getHash(key string, seed int64) uint64 {
	hash := murmur.Murmur3_64([]byte(key), uint64(seed))
	return hash
}

// getBucketIndex returns the index of the bucket where the hash falls in
func getBucketIndex(hash, width uint64) uint64 {
	return hash % width
}

// Capacity returns the total capacity of the scalable bloom filter
func (bf *BloomFilter) Capacity() int {
	return bf.capacity
}

// Close flushes the file to disk and closes the file handle to the filter
func (bf *BloomFilter) Close() error {
	if err := bf.mem.Flush(); err != nil {
		_ = bf.memFile.Close()
		return err
	}
	if err := bf.unmap(); err != nil {
		_ = bf.memFile.Close()
		return err
	}
	if bf.flock != nil {
		err := bf.flock.Unlock()
		if err != nil {
			return err
		}
	}

	if bf.memFile != nil {
		return bf.memFile.Close()
	}
	return nil
}

// Count returns the number of items added to the bloom filter
func (bf *BloomFilter) Count() int {
	return bf.count
}

// FilterSize returns the size of the bloom filter
func (bf *BloomFilter) FilterSize() int {
	return bf.bit_width
}

// DB returns the underlying persistent store
func (bf *BloomFilter) DB() interface{} {
	return bf.db.DB()
}

// Clear resets all bits in the bloom filter
func (bf *BloomFilter) Clear() {
	mem := make([]byte, bf.bit_width)
	copy(bf.mem, mem)
	err := bf.mem.Flush()
	if err != nil {
		fmt.Printf("Error flushing filter to disk: %s\n", err)
		os.Exit(1)
	}
	bf.count = 0
}

type BloomFilterStats struct {
	Capacity int
	Count    int
	Size     int
	M        int
	K        int

	// Prob is the error probability of the filter
	Prob float64
}

// Stats returns the stats of the bloom filter
func (bf *BloomFilter) Stats() BloomFilterStats {
	return BloomFilterStats{
		Capacity: bf.capacity,
		Count:    bf.count,
		Size:     bf.bit_width,
		M:        bf.m,
		K:        bf.k,
		Prob:     bf.err_rate,
	}
}

func (bf *BloomFilter) unmap() error {
	var err error
	if bf.mem != nil {
		err = bf.mem.Unmap()
		if err != nil {
			return err
		}
	}
	return nil
}

// mmap opens the filter file and maps it into memory
func (bf *BloomFilter) mmap() error {

	var err error

	if err := bf.memFile.Truncate(int64(bf.opts.dataSize)); err != nil {
		log.Fatalf("Error truncating file: %s", err)
	}

	bf.mem, err = mmap.MapRegion(bf.memFile, bf.opts.dataSize, mmap.RDWR, 0, 0)
	if err != nil {
		return fmt.Errorf("unable to mmap bloom filter file: %s", err)
	}

	return nil
}

// openFile opens the filter file and locks it
func (bf *BloomFilter) openFile() error {
	var err error
	bf.memFile, err = os.OpenFile(bf.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("unable to open bloom filter file: %s", err)
	}

	if err := bf.flock.TryLock(); err != nil {
		if err == fslock.ErrLocked {
			return fmt.Errorf("file is locked by another process")
		}
		return fmt.Errorf("unable to lock bloom filter file: %s", err)
	}

	return nil
}

// divmod returns the quotient and remainder of a/b
func divmod(num, denom int64) (quot, rem int64) {
	quot = num / denom
	rem = num % denom
	return
}
