package bloom

import (
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
		key, val := "foo", []byte("var")
		bf.Add(key, val)
	})
}
func TestBloomFilter_AddToDB(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	bf := NewBloom(0.01, 1000, store)

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
func TestBloomFilter_AddToBadgerDB(t *testing.T) {
	store, cleanupFunc := BadgerDBSetupTest(t)
	defer cleanupFunc()
	bf := NewBloom(0.01, 1000, store)

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

func TestBloomFilter(t *testing.T) {
	store, cleanupFunc := DBSetupTest(t)
	defer cleanupFunc()
	bf := NewBloom(0.01, 10000, store)
	bf.Add("foo", []byte("bar"))
	bf.Add("baz", []byte("qux"))

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
			found := bf.Find(tt.key)
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
