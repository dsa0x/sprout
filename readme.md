### Sprout

A bloom filter is a probabilistic data structure that is used to determine if an element is present in a set. Bloom filters are fast and space efficient. They allow for false positives, but mitigate the probability with an expected false positive rate. An error rate of 0.001 implies that the probability of a false positive is 1 in 1000.

To fulfil the false positive rate, bloom filters are initialized with a capacity. The capacity is the number of elements that can be inserted into the bloom filter, and this cannot be changed.

Sprout implements a bloom filter in Go, while using boltdb and badgerdb as optional in-memory persistent storage. Sprout writes the bloom filter to a memory-mapped file.

Sprout also implement a scalable bloom filter described in a paper written by [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf).

A scalable bloom filter allows you to grow the filter beyond the initial filter capacity, while preserving the desired false positive rate.

### Memory Usage

Bloom filters are space efficient, as they are only storing the bits that are set. For a filter with a capacity of 20,000,000 and a error rate of 0.001, the storage size is approximately 34MB. That implies approx 1.78 bytes (~14 bits) per element.
The number of bits per element is as a result of the number of hash functions derived from the capacity and the error rate.

**Scalable Bloom Filters**
The scalable bloom filter initialized with a capacity of 2,000,000 and a error rate of 0.001, when grown to a capacity of 20,000,000, the total storage size is approximately 37.3MB.

### Installation

```shell
go get github.com/dsa0x/sprout
```

### Usage

sprout contains implementation of both the normal and scalable bloom filter via the methods below:

```go
opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Capacity: 100000,
	}

bf := sprout.NewBloom(opts)
sbf := sprout.NewScalableBloom(opts)
```

#### With a persistent store

sprout supports boltdb and badgerdb as persistent store. Using them is very simple. sprout exposes methods that initializes the database and then they can be attached to the bloom filter

##### Using Boltdb

```go
// initialize boltdb
db := sprout.NewBolt("/tmp/test.db", 0600)

// you can also setup options supported by bolt to configure your store
opts := bbolt.Options{
		Timeout: 10 * time.Second,
	}
db = sprout.NewBolt("/tmp/test.db", 0600, opts)
defer db.Close()

opts := &sprout.BloomOptions{
		Err_rate: 0.01,
		Path:     "bloom.db",
		Capacity: 100,
	}
bf := sprout.NewBloom(opts)
```

#### Using Scalable bloom filter

```go
// initialize badgerdb
sbf := sprout.NewScalableBloom(sprout.DefaultOptions("/tmp/test.db"))
```

### Example

```go
package main

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/dsa0x/sprout"
)

func main() {

	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "bloom.db",
		Capacity: 100000,
	}
	bf := sprout.NewBloom(opts)
	defer bf.Close()
	bf.Add([]byte("foo"), []byte("bar"))
	fmt.Println(bf.Contains([]byte("foo")))

	// with a persistent store
	opts := badger.DefaultOptions("/tmp/store.db")
	db := sprout.NewBadger(opts)
	bf := sprout.NewScalableBloom(0.9, 100, db)

	bf.Add([]byte("key"), []byte("bar"))
	fmt.Printf("%s\n", bf.Get([]byte("key")))
}
```

#### References

1. [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf)
