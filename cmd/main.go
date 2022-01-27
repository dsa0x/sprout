package main

import (
	"fmt"
	"time"

	bloom "github.com/dsa0x/bloomdb"
)

func main() {

	db := bloom.NewBolt("/tmp/test.db", 0600)
	defer db.Close()
	// db := bloom.NewBadger()
	// blm := bloom.NewBloom(0.01, 100, db)
	blm := bloom.NewScalableBloom(0.1, 100, db)

	start := time.Now()
	for i := 0; i < 200; i++ {
		blm.Add(fmt.Sprintf("foo%d", i), []byte("bar"))
	}
	fmt.Println(time.Since(start), blm.Capacity())

	// fmt.Println(blm.Find("foo"))
	// fmt.Println(blm.Find("baz"))
	// fmt.Println(blm.Find("ese"))

}
