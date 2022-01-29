package gobloomgo

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v3"
)

func TestBadgerDB(t *testing.T) {
	db := NewBadger()

	t.Run("it successfully puts a new value", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		err := db.Put(key, val)
		if err != nil {
			t.Errorf("Expected no error when a value is put in the db, got %v", err)
		}
	})

	t.Run("it gets the previously inserted value", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		db.Put(key, val)

		val, err := db.Get(key)
		if err != nil {
			t.Errorf("Expected no error when a value is retrieved from the db, got %v", err)
		}
		if string(val) != "var" {
			t.Errorf("Expected to get value 'var', got %s", val)
		}
	})
}
func TestBadgerDB_WithOptions(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/badger%d.db", rand.Int())
	db := NewBadger(badger.DefaultOptions(tmpDir))

	t.Run("it successfully puts a new value", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		err := db.Put(key, val)
		if err != nil {
			t.Errorf("Expected no error when a value is put in the db, got %v", err)
		}
	})

	t.Run("it gets the previously inserted value", func(t *testing.T) {
		key, val := []byte("foo"), []byte("var")
		db.Put(key, val)

		val, err := db.Get(key)
		if err != nil {
			t.Errorf("Expected no error when a value is retrieved from the db, got %v", err)
		}
		if string(val) != "var" {
			t.Errorf("Expected to get value 'var', got %s", val)
		}
	})
}
