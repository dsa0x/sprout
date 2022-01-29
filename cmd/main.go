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

	num := 18232
	// bf := gobloomgo.NewBloom(0.1, 50000, nil)
	bf := gobloomgo.NewScalableBloom(0.001, num, nil)

	for i := 0; i < num; i++ {
		var by [4]byte
		binary.LittleEndian.PutUint32(by[:], uint32(i))
		bf.Add(by[:], []byte("bar"))
	}
	bf.Add([]byte("foo"), []byte("var"))

	fmt.Printf("Count: %d, Capacity: %d,\n", bf.Count(), bf.Capacity())
	// fmt.Println(bf.Capacity(), bf.ExpCapacity(), bf.Count(), bf.Prob())

}
