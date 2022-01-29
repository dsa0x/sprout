package main

import (
	"fmt"
	"time"

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

	// bf := gobloomgo.NewBloom(0.1, 1000, nil)
	bf := gobloomgo.NewScalableBloom(0.1, 1000, nil)

	mp := map[bool]int{}

	start := time.Now()
	bf.Add([]byte("key"), []byte("bar"))
	for i := 0; i < 100000; i++ {
		bf.Add([]byte(fmt.Sprintf("foo%d", i)), []byte("bar"))
	}
	for i := 0; i < 200000; i++ {
		mp[bf.Find([]byte(fmt.Sprintf("foo%d", i)))] += 1
	}
	fmt.Printf("%s %v\n", bf.Get([]byte("key")), mp)
	fmt.Println(bf.Capacity(), time.Since(start))

}
