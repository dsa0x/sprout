package sprout

import (
	"fmt"
	"math"
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
}

type GrowthRate uint

var (
	// GrowthRateSmall represents a small expected set growth
	GrowthRateSmall GrowthRate = 2
	// GrowthRateLarge represents a large expected set growth
	GrowthRateLarge GrowthRate = 4
)

// NewScalableBloom creates a new scalable bloom filter.
// err_rate is the desired error rate.
// initial_capacity is the initial capacity of the bloom filter. When the number
// of items exceed the initial capacity, a new filter is created.
//
// The growth rate defaults to 2.
func NewScalableBloom(opts *BloomOptions) *ScalableBloomFilter {
	if opts.Err_rate <= 0 || opts.Err_rate >= 1 {
		panic("Error rate must be between 0 and 1")
	}
	if opts.Capacity < 0 {
		panic("Initial capacity must be greater than 0")
	}
	if opts.GrowthRate == 0 {
		opts.GrowthRate = int(GrowthRateSmall)
	}

	if opts.Path == "" {
		opts.Path = "/tmp/bloom.db"
	}

	initialFilter := NewBloom(opts)
	return &ScalableBloomFilter{
		err_rate:    opts.Err_rate,
		capacity:    opts.Capacity,
		growth_rate: GrowthRate(opts.GrowthRate),
		ratio:       0.9, // Source: [1]
		m0:          initialFilter.m,
		filters:     []*BloomFilter{initialFilter},
		db:          opts.Database,
		path:        opts.Path,
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
	newCapacity := sbf.getNewCap()
	opts := &BloomOptions{
		Err_rate: err_rate,
		Capacity: newCapacity,
		Database: sbf.db,
		Path:     sbf.path + fmt.Sprint(len(sbf.filters)),
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

// Count returns the number of items added to the bloom filter
func (sbf *ScalableBloomFilter) Count() int {
	sum := 0
	for _, filter := range sbf.filters {
		sum += filter.count
	}
	return sum
}
func (sbf *ScalableBloomFilter) bitWidth() int {
	sum := 0
	for _, filter := range sbf.filters {
		sum += filter.bit_width
	}
	return sum
}

func (sbf *ScalableBloomFilter) prob() float64 {
	sum := 1.0
	for i, _ := range sbf.filters {
		sum *= 1.0 - (sbf.err_rate * math.Pow(sbf.ratio, float64(i)))
	}
	return 1.0 - sum
}

func (sbf *ScalableBloomFilter) expCapacity() float64 {
	sum := 0
	for i, _ := range sbf.filters {
		sum += int(math.Pow(float64(sbf.growth_rate), float64(i)))
	}
	return float64(sum*sbf.m0) * math.Ln2
}
