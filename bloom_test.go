package sprout

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/dgraph-io/badger/v3"
)

func DBSetupTest(t *testing.T) (Store, func()) {
	tempfile := fmt.Sprintf("%s/test.db", t.TempDir())
	db := NewBolt(tempfile, 0600)

	return db, func() {
		os.Remove(tempfile)
		db.Close()
	}
}
func BadgerDBSetupTest(t *testing.T) (Store, func()) {
	tempfile := fmt.Sprintf("%s/test.db", t.TempDir())
	opts := badger.DefaultOptions(tempfile)
	opts.WithValueLogFileSize(10240)
	db := NewBadger(opts)

	return db, func() {
		os.Remove(tempfile)
		db.Close()
	}
}

func TestBloomFilter_Add(t *testing.T) {
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 1000,
		Path:     "./test.db",
	}
	bf := NewBloom(opts)

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

	t.Run("success", func(t *testing.T) {
		key := []byte("foo")
		bf.Add(key)
	})

	t.Run("count should sum up to the number of entries added", func(t *testing.T) {
		opts := &BloomOptions{
			Err_rate: 0.01,
			Capacity: 110000,
			Path:     "./test1.db",
		}
		bf := NewBloom(opts)

		defer func() {
			bf.Close()
			os.Remove(opts.Path)
		}()

		count := 100000
		for i := 0; i < count; i++ {
			var by [4]byte
			binary.LittleEndian.PutUint32(by[:], uint32(i))
			bf.Add(by[:])
		}
		if bf.Count() != count {
			t.Errorf("Expected count to be %d, got %d", bf.Count(), count)
		}
	})

	t.Run("add should panic when number of entries exceed the capacity", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				fmt.Println("recovered from panic")
				return
			}
		}()

		count := 1000
		opts := &BloomOptions{
			Err_rate: 0.01,
			Capacity: 1000,
			Path:     "./test2.db",
		}
		bf := NewBloom(opts)

		defer func() {
			bf.Close()
			os.Remove(opts.Path)
		}()
		for i := 0; i < count; i++ {
			var by [4]byte
			binary.LittleEndian.PutUint32(by[:], uint32(i))
			bf.Add(by[:])
		}
		bf.Add([]byte("test"))
		t.Errorf("Expected function to panic when number of entries exceed the capacity")
	})

	t.Run("get should panic when there is no persistent store", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				fmt.Println("recovered from panic")
				return
			}
		}()

		opts := &BloomOptions{
			Err_rate: 0.1,
			Capacity: 100,
			Path:     "./test3.db",
		}
		bf := NewBloom(opts)

		defer func() {
			bf.Close()
			os.Remove(opts.Path)
		}()

		bf.Put([]byte("foo"), []byte("bar"))
		val := bf.Get([]byte("foo"))
		t.Errorf("Expected function to panic when there is no persistent store, got %s", val)
	})
}

func TestBloomFilter_Merge(t *testing.T) {
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 1000,
		Path:     "./test.db",
	}
	bf := NewBloom(opts)
	opts2 := *opts
	opts2.Path = "./test2.db"
	bf2 := NewBloom(&opts2)

	defer func() {
		bf.Close()
		bf2.Close()
		os.Remove(opts.Path)
		os.Remove(opts2.Path)
	}()

	t.Run("merge success", func(t *testing.T) {
		err := bf.Merge(bf2)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("merge should return an error when the filters dont match", func(t *testing.T) {
		opts := &BloomOptions{
			Err_rate: 0.01,
			Capacity: 10000,
			Path:     "./test3.db",
		}
		bf2 := NewBloom(opts)
		bf.Merge(bf2)
		defer func() {
			bf.Close()
			bf2.Close()
			os.Remove(opts.Path)
		}()

		err := bf.Merge(bf2)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
	})

	t.Run("object added to the single filters should be found in the resulting merge", func(t *testing.T) {
		key := []byte("foo")
		opts := &BloomOptions{
			Err_rate: 0.01,
			Capacity: 1000,
			Path:     "./test4.db",
		}
		bf := NewBloom(opts)
		opts2 := *opts
		opts2.Path = "./test5.db"
		bf2 := NewBloom(&opts2)
		bf2.Add(key)
		err := bf.Merge(bf2)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !bf.Contains(key) {
			t.Errorf("Expected key %s to be found in the merged filter", string(key))
		}
		defer func() {
			bf.Close()
			bf2.Close()
			os.Remove(opts.Path)
			os.Remove(opts2.Path)
		}()
	})

}
func TestBloomFilter_AddToDB(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 1000,
		Database: store,
		Path:     "./test.db",
	}

	bf := NewBloom(opts)

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Put(key, val)

		if val, err := bf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("bf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Put(key, val)

		if val, err := bf.db.Get([]byte("bar")); err != nil || val != nil {
			t.Errorf("expected value to be nil, got %s; error: %v", val, err)
		}
	})
}
func TestBloomFilter_AddToBadgerDB(t *testing.T) {
	store, cleanupFunc := BadgerDBSetupTest(t)
	defer cleanupFunc()
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 1000,
		Database: store,
		Path:     "./test.db",
	}
	bf := NewBloom(opts)
	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Put(key, val)

		if val, err := bf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("bf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Put(key, val)

		if val, err := bf.db.Get([]byte("bar")); err != nil || val != nil {
			t.Errorf("expected value to be nil, got %s; error: %v", val, err)
		}
	})
}

func TestBloomFilter(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 10000,
		Database: store,
		Path:     "./test.db",
	}
	bf := NewBloom(opts)

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

	bf.Add([]byte("foo"))
	bf.Add([]byte("baz"))

	t.Run("key may be in cache if found in bloom, def not in cache if not found", func(t *testing.T) {
		bf.Put([]byte("foo"), []byte("bar"))
		bf.Put([]byte("baz"), []byte("qux"))
		table := []struct {
			key      string
			expected bool
		}{
			{"foo", true},
			{"baz", true},
			{"qux", false},
			{"bar", false},
		}

		for _, tt := range table {
			found := bf.Contains([]byte(tt.key))
			if !found {
				val, err := bf.db.Get([]byte(tt.key))
				if err != nil {
					t.Errorf("Expected Get(%s) to not return error, got error %v", tt.key, err)
				}
				if val != nil && !tt.expected {
					t.Errorf("Expected cache to miss after bloom filter returned negative, found value %s", val)
				}
				if val == nil && tt.expected {
					t.Errorf("Expected to find value for key %s in cache, got %v", tt.key, val)
				}
			}
		}
	})
}

func assertPanic(t *testing.T, fn func()) {
	defer func() {
		if r := recover(); r == nil {
			fmt.Println("recovered from panic")
		}
	}()
	fn()
	t.Errorf("The code did not panic")
}

func TestBloomFilter_Clear(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	opts := &BloomOptions{
		Err_rate: 0.01,
		Capacity: 1000,
		Database: store,
		Path:     "./test.db",
	}
	bf := NewBloom(opts)

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

	for i := 0; i < opts.Capacity/2; i++ {
		bf.Add([]byte(fmt.Sprintf("foo%d", i)))
	}

	keys := []string{"foo", "baz", "bar"}
	for _, key := range keys {
		bf.Add([]byte(key))
	}

	t.Run("should clear the bloom filter", func(t *testing.T) {
		bf.Clear()
		for _, key := range keys {
			if bf.Contains([]byte(key)) {
				t.Errorf("Expected key to not be found in bloom filter after clear")
			}
		}
	})

	for i := 0; i < opts.Capacity/2; i++ {
		bf.Add([]byte(fmt.Sprintf("foo%d", i)))
	}

	for _, key := range keys {
		bf.Add([]byte(key))
		t.Run("Should find the newly added keys", func(t *testing.T) {
			if !bf.Contains([]byte(key)) {
				t.Errorf("Expected key to be found in bloom filter after clear")
			}
		})
	}
}
func TestBloomFilter_FileLock(t *testing.T) {
	t.Run("should clear the bloom filter", func(t *testing.T) {
		store, cleanupFunc := DBSetupTest(t)
		defer cleanupFunc()
		if os.Getenv("SPROUT_LOCK") == "1" {
			opts := &BloomOptions{
				Err_rate: 0.01,
				Capacity: 1000,
				Database: store,
				Path:     "./test.db",
			}
			bf := NewBloom(opts)
			defer func() {
				bf.Close()
				os.Remove(opts.Path)
			}()
			NewBloom(opts)
			return
		}

		cmd := exec.Command(os.Args[0], "-test.run=TestBloomFilter_FileLock")
		cmd.Env = append(os.Environ(), "SPROUT_LOCK=1")
		err := cmd.Run()
		if e, ok := err.(*exec.ExitError); ok && !e.Success() {
			return
		}
		t.Fatalf("expected file lock error, got none")
	})

}
