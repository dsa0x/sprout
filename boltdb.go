package sprout

import (
	"fmt"
	"os"
	"sync"

	bolt "go.etcd.io/bbolt"
)

type BoltStore struct {
	db       *bolt.DB
	opts     *bolt.Options
	filePath string
	fileMode os.FileMode
	name     string
	dblock   sync.Mutex
}

// default temp file path for boltdb
var (
	boltTmpFile = "/tmp/bolt.db"
	bucketName  = "boltstore"
)

// NewBolt instantiates a new BoltStore.
func NewBolt(filePath string, filemode os.FileMode, opts ...bolt.Options) *BoltStore {
	store := &BoltStore{
		filePath: filePath,
		fileMode: filemode,
		dblock:   sync.Mutex{},
		name:     bucketName,
	}

	if store.filePath == "" {
		store.filePath = boltTmpFile
	}

	if len(opts) > 0 {
		store.opts = &opts[0]
	} else {
		store.opts = bolt.DefaultOptions
	}

	err := store.open()
	if err != nil {
		fmt.Printf("failed to open boltdb: %v", err)
		os.Exit(1)
	}

	return store
}

func (store *BoltStore) open() error {
	var err error
	store.db, err = bolt.Open(store.filePath, store.fileMode, store.opts)
	if err != nil {
		return err
	}

	err = store.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(store.name))
		return err
	})
	return err
}

func (store *BoltStore) Close() error {
	return store.db.Close()
}

func (store *BoltStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(store.name))
		value = b.Get(key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (store *BoltStore) Put(key []byte, value []byte) error {
	err := store.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(store.name))
		err := b.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// isReady returns true if the store is ready to use.
func (store *BoltStore) isReady() bool {
	return store.db != nil
}

func (store *BoltStore) DB() interface{} {
	return store.db
}

var _ Store = (*BoltStore)(nil)
