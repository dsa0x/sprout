package main

import (
	"fmt"

	gobloomgo "github.com/dsa0x/gobloomgo"
)

func main() {

	// opts := bbolt.Options{
	// 	Timeout: 10 * time.Second,
	// }
	// db := gobloomgo.NewBolt("/tmp/test.db", 0600, opts)
	// defer db.Close()

	// opts := badger.DefaultOptions("/tmp/bloom.db")
	// db := gobloomgo.NewBadger(opts)

	// blm := gobloomgo.NewBloom(0.01, 100, db)
	bf := gobloomgo.NewScalableBloom(0.001, 100, nil)

	mp := map[bool]int{}

	// start := time.Now()
	bf.Add([]byte("key"), []byte("bar"))
	for i := 0; i < 20000; i++ {
		bf.Add([]byte(fmt.Sprintf("foo%d", i)), []byte("bar"))
	}
	for i := 0; i < 30000; i++ {
		mp[bf.Find([]byte(fmt.Sprintf("foo%d", i)))] += 1
	}
	// bf.FilterSize()
	fmt.Printf("%s %v\n", bf.Get([]byte("key")), mp)
	fmt.Println(bf.Capacity())

}
