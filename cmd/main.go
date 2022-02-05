package main

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dsa0x/gobloomgo"
	"go.etcd.io/bbolt"
)

func main() {
	num := 2_000_000
	bf := gobloomgo.NewBloom2(0.001, num, nil)
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

func main4(num int) {
	db, err := badger.Open(badger.DefaultOptions("badger.db"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	for i := 0; i < num; i++ {
		err = db.Update(func(tx *badger.Txn) error {
			for i := 0; i < num; i++ {
				err := tx.Set([]byte{byte(i)}, []byte(fmt.Sprintf("bar-%d", i)))
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err != nil {
		panic(err)
	}

}

func main3(num int) {
	_ = num
	db, err := bbolt.Open("./test.db", 0600, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%+v", db.Info())
	defer db.Close()

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("mybucket"))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		for i := 0; i < num; i++ {
			b := tx.Bucket([]byte("mybucket"))
			err := b.Put([]byte{byte(i)}, []byte(fmt.Sprintf("bar-%d", i)))
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("mybucket"))
		count := 0
		fmt.Printf("%+v\n", b.Stats())
		for i := 0; i < num; i++ {
			b := tx.Bucket([]byte("mybucket"))
			val := b.Get([]byte{byte(i)})
			if val != nil {
				count++
			}
		}
		fmt.Println("count: ", count)
		return nil
	})

	db.Sync()
}

func main2() {

	// opts := bbolt.Options{
	// 	Timeout: 10 * time.Second,
	// }
	// db := gobloomgo.NewBolt("/tmp/test.db", 0600, opts)
	// defer db.Close()
	// opts := badger.DefaultOptions("/tmp/bloom.db")
	// db := gobloomgo.NewBadger(opts)
	// PrintMemUsage()
	num := 4_000
	// bf := gobloomgo.NewBloom(0.1, num, nil)
	bf := gobloomgo.NewScalableBloom(0.01, 10000, nil)
	_ = bf
	for i := 0; i < num; i++ {
		var by [4]byte
		binary.LittleEndian.PutUint32(by[:], uint32(i))
		bf.Add(by[:], []byte("bar"))
		// if i%40000000 == 0 {
		// 	fmt.Println("added", i)
		// 	PrintMemUsage()
		// }
	}
	start := time.Now()
	for i := 0; i < num; i++ {
		var by [4]byte
		binary.LittleEndian.PutUint32(by[:], uint32(i))
		bf.Find(by[:])
	}
	fmt.Println(time.Since(start))
	// bf.Add([]byte("foo"), []byte("var"))
	PrintMemUsage()

}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB, bytes = %v", bToMb(m.Alloc), m.Alloc)
	fmt.Printf("\tTotalAlloc = %v MiB, bytes = %v", bToMb(m.TotalAlloc), m.TotalAlloc)
	fmt.Printf("\tSys = %v MiB\n", bToMb(m.Sys))
	// fmt.Printf("\tNumGC = %v\n", m.NumGC)
	// fmt.Printf("\tNumGC = %v\n", m.)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
	// return b *
}
