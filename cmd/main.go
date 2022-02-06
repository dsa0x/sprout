package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/dsa0x/sprout"
	bolt "go.etcd.io/bbolt"
)

func main() {
	num := 20_000_00
	// div := num / 10
	// main6()
	// return
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
	fmt.Println(bf.Contains([]byte("bar")))
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
	PrintMemUsage()

}

// Scalable bloom filter
func main2(num int) {
	opts := &sprout.BloomOptions{
		Err_rate: 0.01,
		Path:     "bloom.db",
		Capacity: num,
	}
	bf := sprout.NewScalableBloom(opts)
	start := time.Now()
	for i := 0; i < num*10; i++ {
		bf.Add([]byte{byte(i)})
	}
	bf.Add([]byte("foo"))
	fmt.Println(bf.Contains([]byte("foo")))
	fmt.Println("Added", num*10, "elements in", time.Since(start))
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
