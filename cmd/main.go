package main

import (
	"fmt"

	gobloomgo "github.com/dsa0x/gobloomgo"
)

func main() {

	db := gobloomgo.NewBolt("/tmp/test.db", 0600)
	// db := gobloomgo.NewBadger()
	defer db.Close()
	// blm := gobloomgo.NewBloom(0.01, 100, db)
	bf := gobloomgo.NewScalableBloom(0.9, 100, db)

	// start := time.Now()
	bf.Add("key", []byte("bar"))
	for i := 0; i < 200; i++ {
		bf.Add(fmt.Sprintf("foo%d", i), []byte("bar"))
	}
	bf.FilterSize()
	// fmt.Printf("%s\n", bf.Get([]byte("key")))
	// fmt.Println(time.Since(start), bf.Capacity(), bf.FilterSize())

	// fmt.Println(unsafe.Sizeof(uint8(8)))
	// fmt.Println(unsafe.Sizeof([]bool{false, false, true}))
	// fmt.Println(unsafe.Sizeof(byte('1')))

	// fmt.Println(bf.Find("foo"))
	// fmt.Println(bf.Find("baz"))
	// fmt.Println(bf.Find("ese"))

}
