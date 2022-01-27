package bloom

import (
	"math"
)

// https://haslab.uminho.pt/cbm/files/dbloom.pdf

type ScalableBloomFilter struct {
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
}

type GrowthRate uint

var (
	// GrowthRateSmall represents a small expected growth rate
	GrowthRateSmall GrowthRate = 2
	// GrowthRateLarge represents a large expected growth rate
	GrowthRateLarge GrowthRate = 4
)

// NewScalableBloom creates a new scalable bloom filter. The growth rate defaults to 2.
func NewScalableBloom(err_rate float64, initial_capacity int, database Store, growth_rate ...GrowthRate) *ScalableBloomFilter {
	if err_rate <= 0 || err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if initial_capacity < 0 {
		panic("Initial capacity must be greater than 0")
	}
	_growth_rate := GrowthRateSmall
	if len(growth_rate) > 0 {
		_growth_rate = growth_rate[0]
	}
	// number of hash functions
	numHashFn := int(math.Ceil(math.Log2(1.0 / err_rate)))

	seeds := make([]int64, numHashFn)
	for i := 0; i < len(seeds); i++ {
		seeds[i] = int64((i + 1) << 16)
	}

	initialFilter := NewBloom(err_rate, initial_capacity, database)
	return &ScalableBloomFilter{
		err_rate:    err_rate,
		capacity:    initial_capacity,
		growth_rate: _growth_rate,
		ratio:       0.9,
		m0:          initialFilter.m,
		filters:     []*BloomFilter{initialFilter},
		db:          database,
	}
}

// Add adds a key to the scalable bloom filter
// Complexity: O(k)
func (bf *ScalableBloomFilter) Add(key string, val []byte) {
	if bf.Top().count >= bf.Top().capacity {
		bf.grow()
	}
	bf.Top().Add(key, val)
}

// Find checks if the key is in the bloom filter
// Complexity: O(k*n)
func (bf *ScalableBloomFilter) Find(key string) bool {
	for _, filter := range bf.filters {
		if filter.Find(key) {
			return true
		}
	}
	return false
}

func (bf *ScalableBloomFilter) Get(key []byte) []byte {
	for _, filter := range bf.filters {
		if filter.Find(string(key)) {
			return filter.Get(key)
		}
	}
	return nil
}

// Top returns the top filter in the scalable bloom filter
func (bf *ScalableBloomFilter) Top() *BloomFilter {
	return bf.filters[len(bf.filters)-1]
}

// grow increases the capacity of the bloom filter by adding a new filter
func (bf *ScalableBloomFilter) grow() {
	bf.err_rate = bf.err_rate * bf.ratio

	// newCapacity = m0 * growth_rate^i * ln2
	newCapacity := bf.m0 * int(math.Pow(float64(bf.growth_rate), float64(len(bf.filters))+1.0)*math.Ln2)
	newFilter := NewBloom(bf.err_rate, newCapacity, bf.db)
	bf.filters = append(bf.filters, newFilter)
}

// Size returns the total capacity of the scalable bloom filter
func (bf *ScalableBloomFilter) Capacity() int {
	sum := 0
	for _, filter := range bf.filters {
		sum += filter.capacity
	}
	return sum
}

// FilterSize returns the size of the bloom filter
func (bf *ScalableBloomFilter) FilterSize() int {
	sum := 0
	for _, filter := range bf.filters {
		sum += filter.bit_width
	}
	return sum
}

// getStore returns the store used by the scalable bloom filter
func (bf *ScalableBloomFilter) GetStore() Store {
	return bf.db
}
