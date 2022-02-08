package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dsa0x/sprout"
	bolt "go.etcd.io/bbolt"
)

func main() {

	num := 20_000000
	main1(num)
	// main2(num)

	// main2(num)
}

// Normal bloom filter
func main1(num int) {
	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "bloom.db",
		Capacity: num,
	}
	bf := sprout.NewBloom(opts)
	defer bf.Close()

	// reset filter
	bf.Clear()

	start := time.Now()
	bf.Add([]byte("foo"))

	for i := 0; i < num-2; i++ {
		bf.Add([]byte(fmt.Sprintf("%d", i)))
		// fmt.Println(i+1, bf.Contains([]byte(fmt.Sprintf("%d", i+1))))
	}
	fmt.Println(bf.Contains([]byte("foo")))
	fmt.Println(bf.Contains([]byte("f00")))
	fmt.Printf("%+v\n", bf.Stats())
	fmt.Printf("Added %d elements in %v\n", bf.Capacity(), time.Since(start))
}

// Using sprout with a persistent storage
func main3(num int) {
	db := sprout.NewBolt("store.db", 0600)
	defer db.Close()

	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "/tmp/bloom.db",
		Capacity: num,
		Database: db,
	}

	bf := sprout.NewBloom(opts)
	defer bf.Close()

}

// Scalable bloom filter
func main2(num int) {
	num = num / 10
	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "bloom.db",
		Capacity: num,
	}
	bf := sprout.NewScalableBloom(opts)

	// reset filter
	bf.Clear()

	start := time.Now()
	for i := 1; i < num*10; i++ {
		bf.Add([]byte(fmt.Sprintf("%d", i)))
		// fmt.Println(i+1, bf.Contains([]byte(fmt.Sprintf("%d", i+1))))
	}

	bf.Add([]byte("foo"))
	fmt.Println(bf.Contains([]byte("foo")))
	fmt.Println(bf.Contains([]byte("bar")))
	fmt.Printf("%+v\n", bf.Stats())
	fmt.Println("Started with", num, ", Added", bf.Stats().Count, "elements in", time.Since(start))
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// Misc
func main6() {
	num := 2_000_000
	db, err := bolt.Open("store.db", 0644, nil)
	if err != nil {
		panic(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("test"))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		fmt.Printf("Starting %d: ", i)
		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("test"))
			for j := 0; j < num/10; j++ {
				err := b.Put([]byte(fmt.Sprintf("i%d-j%d", i, j)), []byte("b"))
				if err != nil {
					return err
				}
			}
			fmt.Printf("%+v\n", b.Stats().KeyN)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}
func main7() {
	num := 2_000_000
	db, err := badger.Open(badger.DefaultOptions("badgerstore.db"))
	if err != nil {
		panic(err)
	}
	err = db.Update(func(tx *badger.Txn) error {
		for i := 0; i < num; i++ {
			err := tx.Set([]byte(fmt.Sprintf("i%d-j", i)), []byte("b"))
			if err != nil {
				return err
			}
		}
		return err
	})
	for i := 0; i < 100; i++ {
		err = db.Update(func(tx *badger.Txn) error {
			for j := 0; j < num/100; j++ {
				err := tx.Set([]byte(fmt.Sprintf("i%d-j%d", i, j)), []byte("b"))
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			break
		}
	}
	if err != nil {
		log.Fatal(err)
	}
}
