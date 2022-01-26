package bloom

import (
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type BoltStore struct {
	db     *bolt.DB
	name   string
	dblock sync.Mutex
	// dbrwlock sync.RWMutex
}

var defaultBoltStoreOptions = &StoreOptions{
	Filemode: 0666,
	boltOptions: &bolt.Options{
		Timeout: 1 * time.Second,
	},
	BucketName: "boltstore",
}

func NewBolt() *BoltStore {
	return &BoltStore{
		dblock: sync.Mutex{},
	}
}

func (store *BoltStore) Open(filename string, config *StoreOptions) error {

	if config == nil {
		config = defaultBoltStoreOptions
	}

	store.name = config.BucketName

	var err error
	store.db, err = bolt.Open(filename, config.Filemode, config.boltOptions)
	if err != nil {
		return err
	}
	return nil
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

func (store *BoltStore) Put(key, value []byte) ([]byte, error) {
	err := store.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(store.name))
		if err != nil {
			return err
		}
		err = b.Put(key, value)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (store *BoltStore) IsReady() bool {
	return store.db != nil
}
