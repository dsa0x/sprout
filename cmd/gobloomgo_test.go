package main

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dsa0x/gobloomgo"
	"go.etcd.io/bbolt"
)

func Benchmark_NewBloom(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	bf := gobloomgo.NewBloom(0.001, b.N, nil)
	defer bf.Close()

	for i := 0; i < b.N; i++ {
		bf.Add([]byte{byte(i)}, []byte("bar"))
	}
	n := 0
	for i := 0; i < b.N; i++ {
		bf.Find([]byte{byte(n)})
		n++
	}

}
func Benchmark_NewBloom2(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	bf := gobloomgo.NewBloom2(0.001, b.N, nil)
	defer bf.Close()

	for i := 0; i < b.N; i++ {
		bf.Add([]byte{byte(i)}, []byte("bar"))
	}
	n := 0
	for i := 0; i < b.N; i++ {
		bf.Find([]byte{byte(n)})
		n++
	}

}

func Benchmark_Boltdb(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	db, err := bbolt.Open("test.db", 0600, nil)
	if err != nil {
		return
	}
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
	n := 0
	err = db.Update(func(tx *bbolt.Tx) error {
		for i := 0; i < b.N; i++ {
			b := tx.Bucket([]byte("mybucket"))
			err := b.Put([]byte{byte(n)}, []byte("bar"))
			if err != nil {
				return err
			}
			n++
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}
}
func Benchmark_Badgerdb(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	db, err := badger.Open(badger.DefaultOptions("/tmp/test.db"))
	if err != nil {
		return
	}
	defer db.Close()
	err = db.Update(func(tx *badger.Txn) error {
		n := 0
		for i := 0; i < b.N; i++ {
			err := tx.Set([]byte{byte(n)}, []byte("bar"))
			n++
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
}
