package main

import (
	"testing"
)

func TestFnvHash(t *testing.T) {

	t.Run("success", func(t *testing.T) {
		val := "hello"
		_, err := fnvHash(val)
		if err != nil {
			t.Errorf("error occured when fnvHash(%s) : %s", val, err)
		}
	})
}

func TestBloomFilter_Add(t *testing.T) {
	bf := NewBloom(90, 1000)

	t.Run("success", func(t *testing.T) {
		key, val := "foo", "var"
		err := bf.Add(key, val)
		if err != nil {
			t.Errorf("error occured when bf.Add(%s, %s) : %s", key, val, err)
		}

		if _, ok := bf.cache[key]; !ok {
			t.Errorf("bf.cache[%s] not found", key)
		}
	})
}
