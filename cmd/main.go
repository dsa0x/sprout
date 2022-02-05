package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/dsa0x/sprout"
)

func main() {
	num := 20_000_000
	// main2(num / 10)
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
		// fmt.Println(i+10, bf.Find([]byte(fmt.Sprintf("%d", i+1))))
	}
	fmt.Println(bf.Find([]byte("foo")))
	fmt.Println(bf.Find([]byte("bar")))
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
	fmt.Println(bf.Find([]byte("foo")))
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
