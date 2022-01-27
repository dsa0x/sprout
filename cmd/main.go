package main

import (
	"fmt"

	bloom "github.com/dsa0x/bloomdb"
)

func main() {

	db := bloom.NewBolt("/tmp/test.db", 0600)
	// db := bloom.NewBadger()
	defer db.Close()
	// blm := bloom.NewBloom(0.01, 100, db)
	bf := bloom.NewScalableBloom(0.9, 100, db)

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
