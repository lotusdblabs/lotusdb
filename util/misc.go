package util

import (
	"hash/fnv"
	"math/rand"
)

var (
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return []byte("lotusdb-test-value-" + string(b))
}

func FNV32Hash(key []byte) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write(key)
	return hash.Sum32()
}
