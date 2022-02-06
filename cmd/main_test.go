package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dsa0x/sprout"
	"go.etcd.io/bbolt"
)

func Benchmark_InitializeBloom(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "/tmp/bloom.db",
		Capacity: b.N,
	}
	bf := sprout.NewBloom(opts)

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

}
func Benchmark_NewBloom(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "/tmp/bloom.db",
		Capacity: b.N,
	}
	bf := sprout.NewBloom(opts)
	n := 0
	for i := 0; i < b.N; i++ {
		bf.Add([]byte{byte(n)})
		n++
	}

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

}
func Benchmark_NewBloomFind(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "/tmp/bloom.db",
		Capacity: b.N,
	}
	bf := sprout.NewBloom(opts)

	n := 0
	for i := 0; i < b.N; i++ {
		bf.Add([]byte{byte(n)})
		n++
	}
	n = 0
	for i := 0; i < b.N; i++ {
		bf.Contains([]byte{byte(n)})
		n++
	}

	defer func() {
		bf.Close()
		os.Remove(opts.Path)
	}()

}
func Benchmark_NewBloom2(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	opts := &sprout.BloomOptions{
		Err_rate: 0.001,
		Path:     "/tmp/bloom.db",
		Capacity: b.N,
	}
	bf := sprout.NewBloom2(opts)
	defer bf.Close()

	n := 0
	for i := 0; i < b.N; i++ {
		bf.Add([]byte{byte(n)}, []byte("bar"))
		n++
	}
	n = 0
	for i := 0; i < b.N; i++ {
		bf.Contains([]byte{byte(n)})
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

	defer func() {
		os.Remove("test.db")
	}()
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

	defer func() {
		os.Remove("/tmp/test.db")
	}()
}
