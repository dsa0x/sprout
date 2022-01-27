package bloom

import (
	"os"

	badger "github.com/dgraph-io/badger/v3"
	bolt "go.etcd.io/bbolt"
)

type Store interface {
	open() error
	Close() error
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	IsReady() bool
}

type StoreOptions struct {
	BucketName string
	Filemode   os.FileMode
	boltOpts   *bolt.Options
	badgerOpts *badger.Options
}
