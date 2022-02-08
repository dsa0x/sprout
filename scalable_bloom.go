package sprout

import (
	"log"
	"math"
	"sync"
)

type ScalableBloomFilter struct {
	// The desired false positive rate
	err_rate float64

	// the number of items intended to be added to the bloom filter
	capacity int
	db       Store
	filters  []*BloomFilter
	ratio    float64

	// the number of bits per slice(hashFn) for the first filter
	m0 int

	// growth rate is the rate at which the capacity of the bloom filter grows
	growth_rate GrowthRate

	path string
	opts *BloomOptions
	lock *sync.RWMutex
}

type GrowthRate uint

var (
	// GrowthSmall represents a small expected set growth
	GrowthSmall GrowthRate = 2
	// GrowthLarge represents a large expected set growth
	GrowthLarge GrowthRate = 4
)

// NewScalableBloom creates a new scalable bloom filter.
// err_rate is the desired false error rate. e.g. 0.001 implies 1 false positive in 1000 lookups
// initial_capacity is the initial capacity of the bloom filter. When the number
// of items exceed the initial capacity, a new filter is created.
//
// The growth rate defaults to 2.
func NewScalableBloom(opts *BloomOptions) *ScalableBloomFilter {
	if opts.Err_rate <= 0 || opts.Err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if opts.Capacity <= 0 {
		panic("Initial capacity must be greater than 0")
	}
	if opts.GrowthRate == 0 {
		opts.GrowthRate = GrowthSmall
	}

	if opts.Path == "" {
		opts.Path = "/tmp/bloom.db"
	}

	initialFilter := NewBloom(opts)
	return &ScalableBloomFilter{
		err_rate:    opts.Err_rate,
		capacity:    opts.Capacity,
		growth_rate: opts.GrowthRate,
		ratio:       0.9, // Source: [1]
		m0:          initialFilter.m,
		filters:     []*BloomFilter{initialFilter},
		db:          opts.Database,
		path:        opts.Path,
		opts:        opts,
		lock:        &sync.RWMutex{},
	}
}

// Add adds a key to the scalable bloom filter
// Complexity: O(k)
func (sbf *ScalableBloomFilter) Add(key []byte) {
	sbf.add(key)
}

func (sbf *ScalableBloomFilter) add(key []byte) {
	if sbf.Top().count >= sbf.Top().capacity {
		sbf.grow()
	}

	// the top filter is the one holding the mmaped bytes
	bf := sbf.Top()

	indices := bf.candidates(string(key))

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])
		if int(idx) >= bf.bit_width {
			panic("Error adding key: Index out of bounds")
		}
		bf.mem[bf.pageOffset+int(idx)] |= mask
	}
	bf.count++
}

// Put adds a key to the scalable bloom filter, and puts the value in the database
func (sbf *ScalableBloomFilter) Put(key, val []byte) error {
	sbf.add(key)
	return sbf.db.Put(key, val)
}

// Contains checks if the key is in the bloom filter
// Complexity: O(k*n)
func (sbf *ScalableBloomFilter) Contains(key []byte) bool {
	for _, filter := range sbf.filters {
		if sbf.contains(filter, key) {
			return true
		}
	}
	return false
}

func (sbf *ScalableBloomFilter) contains(bf *BloomFilter, key []byte) bool {
	topFilter := sbf.Top()

	indices := bf.candidates(string(key))

	for i := 0; i < len(indices); i++ {
		idx, mask := bf.getBitIndexN(indices[i])

		if int(idx) >= bf.bit_width {
			panic("Error finding key: Index out of bounds")
		}
		if bit := topFilter.mem[bf.pageOffset+int(idx)]; bit&mask == 0 {
			return false
		}
	}
	return true
}

// Get returns the value associated with the key
func (sbf *ScalableBloomFilter) Get(key []byte) []byte {
	for _, filter := range sbf.filters {
		if filter.Contains(key) {
			return filter.Get(key)
		}
	}
	return nil
}

// Top returns the top filter in the scalable bloom filter
func (sbf *ScalableBloomFilter) Top() *BloomFilter {
	return sbf.filters[len(sbf.filters)-1]
}

// grow increases the capacity of the bloom filter by adding a new filter
func (sbf *ScalableBloomFilter) grow() {

	// unmap the old top filter
	err := sbf.Top().Close()
	if err != nil {
		log.Panicf("Error unmapping top filter before grow: %v", err)
	}

	err_rate := sbf.err_rate * math.Pow(sbf.ratio, float64(len(sbf.filters)))
	newCapacity := sbf.getNewCap()
	opts := &BloomOptions{
		Err_rate: err_rate,
		Capacity: newCapacity,
		Database: sbf.db,
		Path:     sbf.path,
		dataSize: sbf.Top().bit_width,
	}
	newFilter := NewBloom(opts)
	sbf.filters = append(sbf.filters, newFilter)
}

func (sbf *ScalableBloomFilter) getNewCap() int {
	i := float64(len(sbf.filters)) - 1.0
	newCapacity := float64(sbf.m0) * float64(math.Pow(float64(sbf.growth_rate), i)) * math.Ln2
	return int(newCapacity)
}

// Size returns the total capacity of the scalable bloom filter
func (sbf *ScalableBloomFilter) Capacity() int {
	sum := 0
	for _, filter := range sbf.filters {
		sum += filter.capacity
	}
	return sum
}

// filterSize returns the total filter size
func (sbf *ScalableBloomFilter) filterSize() int {
	return sbf.Top().bit_width
}

// DB returns the store used by the scalable bloom filter
func (sbf *ScalableBloomFilter) DB() Store {
	return sbf.db
}

// Count returns the number of items added to the bloom filter
func (sbf *ScalableBloomFilter) Count() int {
	sum := 0
	for _, filter := range sbf.filters {
		sum += filter.count
	}
	return sum
}

// Close closes the scalable bloom filter
func (sbf *ScalableBloomFilter) Close() error {
	return sbf.Top().Close()
}

func (sbf *ScalableBloomFilter) prob() float64 {
	sum := 1.0
	for i := range sbf.filters {
		sum *= 1.0 - (sbf.err_rate * math.Pow(sbf.ratio, float64(i)))
	}
	return 1.0 - sum
}

// Stats returns the stats of the bloom filter
func (sbf *ScalableBloomFilter) Stats() BloomFilterStats {
	return BloomFilterStats{
		Capacity: sbf.Capacity(),
		Count:    sbf.Count(),
		Size:     sbf.filterSize(),
		M:        sbf.Top().m,
		K:        sbf.Top().k,
		Prob:     sbf.prob(),
	}
}

// Clear resets all bits in the bloom filter
func (sbf *ScalableBloomFilter) Clear() {
	sbf.lock.Lock()
	defer sbf.lock.Unlock()
	err := sbf.Top().Close()
	if err != nil {
		log.Fatalf("Error closing top filter before clear: %v", err)
	}
	sbf.filters = []*BloomFilter{NewBloom(sbf.opts)}

}
