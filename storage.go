package sprout

type Store interface {
	open() error
	Close() error
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	isReady() bool
	DB() interface{}
}
