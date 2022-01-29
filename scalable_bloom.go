package gobloomgo

import (
	"math"
)

// https://haslab.uminho.pt/cbm/files/dbloom.pdf

type ScalableBloomFilter struct {
	// The desired false positive rate. e.g. 0.1 error rate implies 1 in 1000
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

// NewScalableBloom creates a new scalable bloom filter.
// err_rate is the desired false positive rate. e.g. 0.1 error rate implies 1 in 1000
// initial_capacity is the initial capacity of the bloom filter. When the number
// of items exceed the initial capacity, a new filter is created.
//
// The growth rate defaults to 2.
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
func (sbf *ScalableBloomFilter) Add(key, val []byte) {
	if sbf.Top().count >= sbf.Top().capacity {
		sbf.grow()
	}
	sbf.Top().Add(key, val)
}

// Find checks if the key is in the bloom filter
// Complexity: O(k*n)
func (sbf *ScalableBloomFilter) Find(key []byte) bool {
	for _, filter := range sbf.filters {
		if filter.Find(key) {
			return true
		}
	}
	return false
}

func (sbf *ScalableBloomFilter) Get(key []byte) []byte {
	for _, filter := range sbf.filters {
		if filter.Find(key) {
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
	err_rate := sbf.err_rate * math.Pow(sbf.ratio, float64(len(sbf.filters)))

	// newCapacity = m0 * growth_rate^i * ln2
	newCapacity := sbf.m0 * int(math.Pow(float64(sbf.growth_rate), float64(len(sbf.filters))+1.0)*math.Ln2)
	newFilter := NewBloom(err_rate, newCapacity, sbf.db)
	sbf.filters = append(sbf.filters, newFilter)
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
	sum := 0
	for _, filter := range sbf.filters {
		sum += filter.bit_width
	}
	return sum
}

// getStore returns the store used by the scalable bloom filter
func (sbf *ScalableBloomFilter) getStore() Store {
	return sbf.db
}
