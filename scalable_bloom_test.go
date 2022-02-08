package sprout

import (
	"fmt"
	"os"
	"testing"
)

func TestScalableBloom(t *testing.T) {
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 100,
		Path:     "./test.db",
	}
	sbf := NewScalableBloom(opts)
	defer sbf.Close()

	t.Run("success", func(t *testing.T) {
		key := []byte("foo")
		sbf.Add(key)
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
	defer sbf.Close()

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Put(key, val)

		if val, err := sbf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("sbf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Put(key, val)

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
	defer sbf.Close()

	t.Run("should grow filter when capacity is full", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		sbf.Put(key, val)
		for i := 0; i < initialCap*10; i++ {
			sbf.Add([]byte(fmt.Sprintf("foo%d", i)))
		}

		if val, err := sbf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("expected sbf.cache[%s] to be found; got error: %v", key, err)
		}

		if sbf.Capacity() < 1000 {
			t.Errorf("expected sbf.capacity to be greater than %d; got %d", 1000, sbf.Capacity())
		}
	})
}

func Test_CompareBFnSBF(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	initialCap := 1000
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: initialCap,
		Database: store,
		Path:     "./test.db",
	}
	sbf := NewScalableBloom(opts)
	opts2 := &BloomOptions{
		Err_rate: 0.01,
		Capacity: initialCap,
		Database: store,
		Path:     "./test2.db",
	}
	bf := NewBloom(opts2)
	defer sbf.Close()
	defer bf.Close()

	t.Run("bf and a sbf that hasnt been scaled should have the same width", func(t *testing.T) {

		for i := 0; i < initialCap; i++ {
			sbf.Add([]byte(fmt.Sprintf("foo%d", i)))
			bf.Add([]byte(fmt.Sprintf("foo%d", i)))
		}

		if bf.bit_width != sbf.Top().bit_width {
			t.Errorf("expected bf and sbf to have the same bit_width; got %d and %d", bf.bit_width, sbf.Top().bit_width)
		}
		if len(bf.mem) != len(sbf.Top().mem) {
			t.Errorf("expected bf and sbf to have the same bit_width; got %d and %d", bf.bit_width, sbf.Top().bit_width)
		}

		for i := initialCap; i < initialCap*2; i++ {
			sbf.Add([]byte(fmt.Sprintf("foo%d", i)))
		}
		if len(bf.mem) >= len(sbf.Top().mem) {
			t.Errorf("expected sbf to have more memory; got bf: %d and sbf:%d", len(bf.mem), len(sbf.Top().mem))
		}
	})

	defer func() {
		os.Remove("./test.db")
		os.Remove("./test2.db")
	}()
}
