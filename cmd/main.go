package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/dsa0x/sprout"
	bolt "go.etcd.io/bbolt"
)

func main() {
	num := 20_000_000
	// div := num / 10
	// main2(num)
	// return
	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "bloom.db",
		Capacity: num,
	}
	bf := sprout.NewBloom(opts)
	defer bf.Close()
	// return
	start := time.Now()
	bf.Add([]byte("foo"), []byte("bar"))

	for i := 0; i < num-1; i++ {
		bf.Add([]byte(fmt.Sprintf("%d", i)), []byte("bar"))
		// if i%div == 0 {
		// 	time.Sleep(time.Second * 3)
		// 	fmt.Println(i, "added")
		// }
		// fmt.Println(i+1, bf.Contains([]byte(fmt.Sprintf("%d", i+1))))
	}
	fmt.Println(bf.Contains([]byte("foo")))
	fmt.Println(bf.Contains([]byte("bar")))
	fmt.Printf("Added %d elements in %v\n", bf.Capacity(), time.Since(start))
	PrintMemUsage()
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
		bf.Add([]byte{byte(i)}, []byte("bar"))
	}
	bf.Add([]byte("foo"), []byte("bar"))
	fmt.Println(bf.Contains([]byte("foo")))
	fmt.Println("Added", num*10, "elements in", time.Since(start))
}

func main4(num int) {
	db, err := bolt.Open("store.db", 0600, nil)
	if err != nil {
		panic(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("store.name"))
		return err
	})
	if err != nil {
		panic(err)
	}

	w, err := os.OpenFile("storebolt.db", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	// defer os.Remove("storebolt.db")

	start := time.Now()
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	size := tx.Size()

	b := tx.Bucket([]byte("store.name"))

	for i := 0; i < num; i++ {
		b.Put([]byte{byte(i)}, []byte("bar"))
	}

	// write snapshot to pipe
	go func() {
		defer w.Close()
		_, err := tx.WriteTo(w)
		if err != nil {
			log.Println("Erroring writing to pipe", err)
		}
	}()
	if err != nil {
		panic(err)
	}
	fmt.Println("Added", num, "elements in", time.Since(start), "bytes=", size)
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
