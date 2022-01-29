package main

import (
	"encoding/binary"

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

	// bf := gobloomgo.NewBloom(0.1, 50000, nil)
	bf := gobloomgo.NewScalableBloom(0.000001, 10, nil)

	for i := 0; i < 100000; i++ {
		var by [4]byte
		binary.LittleEndian.PutUint32(by[:], uint32(i))
		bf.Add(by[:], []byte("bar"))
	}
	bf.Add([]byte("foo"), []byte("var"))

	// fmt.Printf("Count: %d, Capacity: %d, ExpCap: %.f\n", bf.Count(), bf.Capacity(), bf.ExpCapacity())
	// fmt.Println(bf.Capacity(), bf.ExpCapacity(), bf.Count(), bf.Prob())

}
