package gobloomgo

import (
	"fmt"
	"testing"
)

func TestScalableBloom(t *testing.T) {
	bf := NewScalableBloom(0.01, 1000, nil)

	t.Run("success", func(t *testing.T) {
		key, val := "foo", []byte("var")
		bf.Add(key, val)
	})
}

func TestScalableBloomFilter_AddToDB(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	bf := NewScalableBloom(0.01, 1000, store)

	t.Run("success", func(t *testing.T) {
		key, val := "foo", []byte("var")
		bf.Add(key, val)

		if val, err := bf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("bf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := "foo", []byte("var")
		bf.Add(key, val)

		if val, err := bf.db.Get([]byte("bar")); err != nil || val != nil {
			t.Errorf("expected value to be nil, got %s; error: %v", val, err)
		}
	})
}
func TestScalableBloomFilter_GrowFilter(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	initialCap := 100
	bf := NewScalableBloom(0.01, initialCap, store)

	t.Run("should grow filter when capacity is full", func(t *testing.T) {
		key, val := "foo", []byte("var")
		bf.Add(key, val)
		for i := 0; i < initialCap*10; i++ {
			bf.Add(fmt.Sprintf("foo%d", i), val)
		}

		if val, err := bf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("expected bf.cache[%s] to be found; got error: %v", key, err)
		}

		if bf.Capacity() < 1000 {
			t.Errorf("expected bf.capacity to be greater than %d; got %d", 1000, bf.Capacity())
		}
	})
	// t.Run("should not find key that was not added", func(t *testing.T) {
	// 	key, val := "foo", []byte("var")
	// 	bf.Add(key, val)

	// 	if val, err := bf.db.Get([]byte("bar")); err != nil || val != nil {
	// 		t.Errorf("expected value to be nil, got %s; error: %v", val, err)
	// 	}
	// })
}
