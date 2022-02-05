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
	// PrintMemUsage()
	// main3()
	// PrintMemUsage()
	// return
	num := 2_000_000
	bf := gobloomgo.NewBloom2(0.001, num, nil)
	defer bf.Close()
	// return
	// start := time.Now()
	bf.Add([]byte("foo"), []byte("bar"))
	// for i := 0; i < num-1; i++ {
	// 	// fmt.Println(i, "Align::::", bf.Find(by[:]))
	// 	bf.Add([]byte{byte(i)}, []byte("bar"))
	// 	fmt.Println(bf.Find([]byte{byte(i)}))
	// 	// if i%4000000 == 0 {
	// 	// 	fmt.Printf("Run %d: ", i)
	// 	// 	PrintMemUsage()
	// 	// }
	// }
	// 	PrintMemUsage()
	fmt.Println(bf.Find([]byte("foo")))
	fmt.Println(bf.Find([]byte("bar")))
	// 	fmt.Println(time.Since(start))
}

func main4() {
	num := 40_000
	db, err := badger.Open(badger.DefaultOptions("badger.db"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	for i := 0; i < num; i++ {
		err = db.Update(func(tx *badger.Txn) error {
			for i := 0; i < num; i++ {
				err := tx.Set([]byte{byte(i)}, []byte(fmt.Sprintf("barjbsdabhsdbabsfdbksdfhjsdfhsfhdjhjsdghjsghjbsdghbjsdgjhbsdghjb-%d", i)))
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

func main3() {
	num := 40_000_00
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

	// n, batchN := 400000, 200000
	// ksize, vsize := 8, 500

	// for i := 0; i < n; i += batchN {
	// 	if err := db.Update(func(tx *bbolt.Tx) error {
	// 		b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
	// 		if err != nil {
	// 			return err
	// 		}
	// 		for j := 0; j < batchN; j++ {
	// 			k, v := make([]byte, ksize), make([]byte, vsize)
	// 			binary.BigEndian.PutUint32(k, uint32(i+j))
	// 			if err := b.Put(k, v); err != nil {
	// 				return err
	// 			}
	// 		}
	// 		return nil
	// 	}); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	err = db.Update(func(tx *bbolt.Tx) error {
		for i := 0; i < num; i++ {
			b := tx.Bucket([]byte("mybucket"))
			err := b.Put([]byte{byte(i)}, []byte(fmt.Sprintf("barjbsdabhsdbabsfdbksdfhjsdfhsfhdjhjsdghjsghjbsdghbjsdgjhbsdghjb-%d", i)))
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

		// for i := 0; i < 100000; i++ {
		// 	b := tx.Bucket([]byte("mybucket"))
		// 	val := b.Get([]byte{byte(i)})
		// 	fmt.Println(string(val))

		// }
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
	// fmt.Printf("Count: %d, Capacity: %d,\n", bf.Count(), bf.Capacity())
	// fmt.Println(bf.Capacity(), bf.ExpCapacity(), bf.Count(), bf.Prob())

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
