package gobloomgo

import (
	"encoding/binary"
	"fmt"
	"os"
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

func TestFnvHash(t *testing.T) {

	t.Run("success", func(t *testing.T) {
		val := "hello"
		_, err := fnvHash(val)
		if err != nil {
			t.Errorf("error occured when fnvHash(%s) : %s", val, err)
		}
	})
}

func TestBloomFilter_Add(t *testing.T) {
	bf := NewBloom(0.01, 1000, nil)

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Add(key, val)
	})

	t.Run("count should sum up to the number of entries added", func(t *testing.T) {
		bf := NewBloom(0.01, 110000, nil)
		count := 100000
		for i := 0; i < count; i++ {
			var by [4]byte
			binary.LittleEndian.PutUint32(by[:], uint32(i))
			bf.Add(by[:], []byte("bar"))
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
		bf := NewBloom(0.01, count, nil)
		for i := 0; i < count; i++ {
			var by [4]byte
			binary.LittleEndian.PutUint32(by[:], uint32(i))
			bf.Add(by[:], []byte("bar"))
		}
		bf.Add([]byte("test"), []byte("bar"))
		t.Errorf("Expected function to panic when number of entries exceed the capacity")
	})
}
func TestBloomFilter_AddToDB(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	bf := NewBloom(0.01, 1000, store)

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Add(key, val)

		if val, err := bf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("bf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Add(key, val)

		if val, err := bf.db.Get([]byte("bar")); err != nil || val != nil {
			t.Errorf("expected value to be nil, got %s; error: %v", val, err)
		}
	})
}
func TestBloomFilter_AddToBadgerDB(t *testing.T) {
	store, cleanupFunc := BadgerDBSetupTest(t)
	defer cleanupFunc()
	bf := NewBloom(0.01, 1000, store)

	t.Run("success", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Add(key, val)

		if val, err := bf.db.Get([]byte(key)); err != nil || val == nil {
			t.Errorf("bf.cache[%s] not found; error: %v", key, err)
		}
	})
	t.Run("should not find key that was not added", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		bf.Add(key, val)

		if val, err := bf.db.Get([]byte("bar")); err != nil || val != nil {
			t.Errorf("expected value to be nil, got %s; error: %v", val, err)
		}
	})
}

func TestBloomFilter(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	bf := NewBloom(0.01, 10000, store)
	bf.Add([]byte("foo"), []byte("bar"))
	bf.Add([]byte("baz"), []byte("qux"))

	t.Run("key may be in cache if found in bloom, def not in cache if not found", func(t *testing.T) {
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
			found := bf.Find([]byte(tt.key))
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
