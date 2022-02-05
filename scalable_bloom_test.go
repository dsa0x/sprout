package gobloomgo

import (
	"fmt"
	"testing"
)

func TestScalableBloom(t *testing.T) {
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 100,
		Path:     "./test.db",
	}
	sbf := NewScalableBloom(opts)

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Add(key, val)
	})
}

func TestScalableBloomFilter_AddToDB(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 1000,
		Database: store,
		Path:     "./test.db",
	}
	sbf := NewScalableBloom(opts)

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Add(key, val)

		if val, err := sbf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("sbf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Add(key, val)

		if val, err := sbf.db.Get([]byte("bar")); err != nil || val != nil {
			t.Errorf("expected value to be nil, got %s; error: %v", val, err)
		}
	})
}
func TestScalableBloomFilter_GrowFilter(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	initialCap := 100
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: initialCap,
		Database: store,
		Path:     "./test.db",
	}
	sbf := NewScalableBloom(opts)

	t.Run("should grow filter when capacity is full", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Add(key, val)
		for i := 0; i < initialCap*10; i++ {
			sbf.Add([]byte(fmt.Sprintf("foo%d", i)), val)
		}

		if val, err := sbf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("expected sbf.cache[%s] to be found; got error: %v", key, err)
		}

		if sbf.Capacity() < 1000 {
			t.Errorf("expected sbf.capacity to be greater than %d; got %d", 1000, sbf.Capacity())
		}
	})
}
