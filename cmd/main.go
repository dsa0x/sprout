package main

import (
	"encoding/binary"
	"fmt"

	"github.com/dsa0x/gobloomgo"
)

func main() {

	// opts := bbolt.Options{
	// 	Timeout: 10 * time.Second,
	// }
	// db := gobloomgo.NewBolt("/tmp/test.db", 0600, opts)
	// defer db.Close()

	// opts := badger.DefaultOptions("/tmp/bloom.db")
	// db := gobloomgo.NewBadger(opts)

	bf := gobloomgo.NewBloom(0.1, 59000, nil)
	// bf := gobloomgo.NewScalableBloom(0.1, 1000, nil)

	// mp := map[bool]int{}

	// start := time.Now()
	// bf.Add([]byte("key"), []byte("bar"))
	ii := 0
	for i := 0; i < 50000; i++ {
		var by [4]byte
		binary.LittleEndian.PutUint32(by[:], uint32(i))
		bf.Add(by[:], []byte("bar"))
		ii++
		count := bf.Count()

		fmt.Println(by, bf.Find(by[:]), count == ii, "bfCo=", count, "ii=", ii)

	}

	// fmt.Println((0 - bf.Count()), bf.Count(), ii)
	// for i := 50000; i < 2000; i++ {
	// 	mp[bf.Find([]byte(fmt.Sprintf("foo%d", i)))] += 1
	// }
	// fmt.Printf("%s %v\n", bf.Get([]byte("key")), mp)
	// fmt.Println(bf.Capacity(), time.Since(start))

}
