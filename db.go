package bloom

import (
	"os"

	bolt "go.etcd.io/bbolt"
)

type Store interface {
	Open(filename string, config *StoreOptions) error
	Close() error
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) ([]byte, error)
	IsReady() bool
}

type StoreOptions struct {
	BucketName  string
	Filemode    os.FileMode
	boltOptions *bolt.Options
}
