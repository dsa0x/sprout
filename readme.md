### sprout

A bloom filter is a probabilistic data structure that is used to determine if an element is present in a set.

Sprout implements a bloom filter in Go, while using boltdb and badgerdb as optional in-memory persistent storage.

Bloomdb also implement a scalable bloom filter described in a paper written by [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf).

A scalable bloom filter allows you to grow the filter as needed, and removes the need for an apriori filter size as expected by the basic bloom filter, while preserving the desired false positive rate by scaling the filter as needed.

Sprout writes the bloom filter to a memory mapped file.

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

bf := sprout.NewBloom(0.01, 100, db)
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
	fmt.Println(bf.Find([]byte("foo")))

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
