package murmur

import "encoding/binary"

// Taken from https://github.com/RedisBloom/RedisBloom/blob/master/contrib/MurmurHash2.c
// MurmurHash2, 64-bit versions, by Austin Appleby

// 64-bit hash for 64-bit platforms

// MurmurHash64A_Bloom returns the murmur hash of the given data
func MurmurHash64A_Bloom(key []byte, length, seed uint64) uint64 {
	m := uint64(0xc6a4a7935bd1e995)
	r := 47

	h := uint64(seed) ^ (length * m)
	data := uint64(key[0])

	for i := 1; i < len(key); i++ {
		switch i {
		case 1:
			data |= uint64(key[i]) << 8
		case 2:
			data |= uint64(key[i]) << 16
		case 3:
			data |= uint64(key[i]) << 24
		case 4:
			data |= uint64(key[i]) << 32
		case 5:
			data |= uint64(key[i]) << 40
		case 6:
			data |= uint64(key[i]) << 48
		case 7:
			data |= uint64(key[i]) << 56
		}
	}

	end := data + uint64(length/8)

	for data != end {
		data++
		k := data

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	data2 := make([]byte, 8)
	binary.PutUvarint(data2, data)

	switch length & 7 {
	case 7:
		h ^= uint64(data2[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(data2[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(data2[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(data2[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(data2[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(data2[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(data2[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}
