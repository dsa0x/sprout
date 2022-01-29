### gobloomgo

A bloom filter is a probabilistic data structure that is used to determine if an element is present in a set.

Bloomdb implements a bloom filter in Go, while using boltdb and badgerdb as optional in-memory persistent storage.

Bloomdb also implement a scalable bloom filter described in a paper written by [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf).

A scalable bloom filter removes the need for an apriori filter size as expected by the basic bloom filter, while preserving the desired false positive rate by scaling the filter as needed.

### Installation

```shell
go get github.com/dsa0x/gobloomgo
```

### Usage

gobloomgo contains implementation of both the normal and scalable bloom filter via the methods below:

```go
bf := gobloomgo.NewBloom(0.01, 100, nil)
sbf := gobloomgo.NewScalableBloom(0.9, 100, nil)
```

#### With a persistent store

gobloomgo supports boltdb and badgerdb as persistent store. Using them is very simple. gobloomgo exposes methods that initializes the database and then they can be attached to the bloom filter

##### Using Boltdb

```go
// initialize boltdb
db := gobloomgo.NewBolt("/tmp/test.db", 0600)

// you can also setup options supported by bolt to configure your store
opts := bbolt.Options{
		Timeout: 10 * time.Second,
	}
db = gobloomgo.NewBolt("/tmp/test.db", 0600, opts)
defer db.Close()

bf := gobloomgo.NewBloom(0.01, 100, db)
```

##### Badgerdb (v3)

```go
// initialize badgerdb
db := gobloomgo.NewBadger()

// initialize badgerdb with options
opts := badger.DefaultOptions("/tmp/bloom.db")
db = gobloomgo.NewBadger(opts)

bf := gobloomgo.NewBloom(0.01, 100, db)
```

#### Using Scalable bloom filter

```go
// initialize badgerdb
db := gobloomgo.NewBadger()

sbf := gobloomgo.NewScalableBloom(0.9, 100, db)
```

### Example

```go
package main

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	gobloomgo "github.com/dsa0x/gobloomgo"
)

func main() {
	opts := badger.DefaultOptions("/tmp/bloom.db")
	db := gobloomgo.NewBadger(opts)
	bf := gobloomgo.NewScalableBloom(0.9, 100, db)

	bf.Add([]byte("key"), []byte("bar"))
	fmt.Printf("%s\n", bf.Get([]byte("key")))
}
```
