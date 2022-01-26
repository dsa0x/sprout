package main

import (
	"fmt"

	bloom "github.com/dsa0x/bloomdb"
)

func main() {

	boltstore := bloom.NewBolt()
	err := boltstore.Open("/tmp/bolt.db", nil)
	if err != nil {
		fmt.Printf("Error opening boltdb: %s\n", err)
		return
	}
	blm := bloom.NewScalableBloom(0.1, 100, bloom.GrowthRateSmall, boltstore)

	for i := 0; i < 1000; i++ {
		blm.Add(fmt.Sprintf("foo%d", i), []byte("bar"))
		fmt.Println(blm.Find(fmt.Sprintf("foo%d", i*2)), fmt.Sprintf("foo%d", i))
	}

	fmt.Println(blm.Capacity())

	// boltdb := NewBolt()
	// err := boltdb.Open("test.db", nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// blm := NewBloom(0.01, 10000, boltdb)
	// blm.Add("foo", []byte("bar"))
	// blm.Add("baz", []byte("qux"))

	// fmt.Println(blm.Find("foo"))
	// fmt.Println(blm.Find("baz"))
	// fmt.Println(blm.Find("ese"))

}
