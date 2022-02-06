### Sprout

A bloom filter is a probabilistic data structure that is used to determine if an element is present in a set. Bloom filters are fast and space efficient. They allow for false positives, but mitigate the probability with an expected false positive rate. An error rate of 0.001 implies that the probability of a false positive is 1 in 1000. Bloom filters don't store the elements themselves, but instead use a set of hash functions to determine the presence of an element.

To fulfil the false positive rate, bloom filters can be initialized with a capacity. The capacity is the number of elements that can be inserted into the bloom filter, and this cannot be changed.

Sprout implements a bloom filter in Go. The bits of the filter are stored in a memory-mapped file. Sprout also allows attaching a persistent storage (boltdb and badgerdb) to store the key value pairs.

Sprout also implement a scalable bloom filter described in a paper written by [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf).

A scalable bloom filter allows you to grow the filter beyond the initial filter capacity, while preserving the desired false positive rate.

### Storage Size

Bloom filters are space efficient, as they only store the bits that are set. For a filter with a capacity of 2,000,000 and a error rate of 0.001, the storage size is approximately 3.4MB. That implies that there are approximately 1.8 bytes (~14 bits) per element.
The number of bits per element is as a result of the number of hash functions, which is derived from the capacity and the error rate.

**Scalable Bloom Filters**: A scalable bloom filter initialized with a capacity of 2,000,000 and an error rate of 0.001, when grown to a capacity of 20,000,000, the total storage size is approximately 37.3MB.

**Comparison to Key-Value stores**

Adding 2 million elements (with a single byte value)

| Database | Size  |
| -------- | ----- |
| BoltDB   | 108MB |
| BadgerDB | 128MB |
| Sprout   | 3.4MB |

### Installation

```shell
go get github.com/dsa0x/sprout
```

### Usage

Sprout contains implementation of both the normal and scalable bloom filter via the methods below:

```go
opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Capacity: 100000,
	}
// Normal Bloom Filter
bf := sprout.NewBloom(opts)

// Scalable Bloom Filter
sbf := sprout.NewScalableBloom(opts)
```

#### With a persistent store

Sprout supports boltdb and badgerdb as persistent storage. Using them is very simple. Sprout exposes methods that initializes the database and then they can be attached to the bloom filter.

**Using Boltdb**

```go
// initialize boltdb
db := sprout.NewBolt("/tmp/test.db", 0600)

// the bolt store can be configured as defined in the boltdb documentations
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
	bf.Add([]byte("foo"))
	fmt.Println(bf.Contains([]byte("foo")))

	// with a persistent store
	badgerOpts := badger.DefaultOptions("/tmp/store.db")
	db := sprout.NewBadger(badgerOpts)
	opts.Database = db
	bf := sprout.NewScalableBloom(opts)

	bf.Put([]byte("key"), []byte("bar"))
	fmt.Printf("%s\n", bf.Get([]byte("key")))
}
```

#### References

1. [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf)
2. [Austin Appleby Murmur hash Source Code](https://github.com/aappleby/smhasher)
