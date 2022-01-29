### gobloomgo

A bloom filter is a probabilistic data structure that is used to determine if an element is present in a set.

Bloomdb implements a bloom filter in Go, using boltdb and badgerdb as optional in-memory persistent storage. Bloomdb also implement a scalable bloom filter described in the publication by [P. Almeida, C.Baquero, N. Preguiça, D. Hutchison](https://haslab.uminho.pt/cbm/files/dbloom.pdf).

A scalable bloom filter removes the need for an apriori filter size as expected by the basic bloom filter, while preserving the desired false positive rate by scaling the filter as needed.

### Installation

```shell
go get github.com/dsa0x/gobloomgo
```

### Usage

gobloomgo contains implementation of both the normal and scalable bloom filter via the methods below:

```go
bf := gobloomgo.NewBloom(0.01, 100, db)
```
