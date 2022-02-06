package sprout

import (
	"fmt"
	"os"
	"sync"

	badger "github.com/dgraph-io/badger/v3"
)

type BadgerStore struct {
	db     *badger.DB
	opts   badger.Options
	dblock sync.Mutex
}

// default temp file path for badgerdb
var badgerTmpFile = "/tmp/badger.db"

// NewBadger instantiates a new BadgerStore.
func NewBadger(opts ...badger.Options) *BadgerStore {
	store := &BadgerStore{
		dblock: sync.Mutex{},
	}

	if len(opts) > 0 {
		store.opts = opts[0]
	} else {
		store.opts = badger.DefaultOptions(badgerTmpFile)
	}

	if store.opts.Dir == "" {
		store.opts = store.opts.WithDir(badgerTmpFile).WithValueDir(badgerTmpFile)
	}

	err := store.open()
	if err != nil {
		fmt.Printf("failed to open badgerdb: %v", err)
		os.Exit(1)
	}
	return store
}

func (store *BadgerStore) open() error {
	var err error
	store.db, err = badger.Open(store.opts)
	if err != nil {
		return err
	}
	return nil
}

func (store *BadgerStore) Close() error {
	return store.db.Close()
}

func (store *BadgerStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := store.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = val
			return nil
		})
		return err
	})

	if err != nil {
		return nil, err
	}
	return value, nil
}

func (store *BadgerStore) Put(key, value []byte) error {
	err := store.db.Update(func(tx *badger.Txn) error {
		err := tx.Set(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// isReady returns true if the store is ready to use.
func (store *BadgerStore) isReady() bool {
	return store.db != nil
}

func (store *BadgerStore) DB() interface{} {
	return store.db
}

var _ Store = (*BadgerStore)(nil)
