package util

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	lock = sync.Mutex{}
	//nolint:gosec //used in test
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func GetTestKey(i int64) []byte {
	return []byte(fmt.Sprintf("lotusdb-test-key-%09d", i))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		lock.Lock()
		b[i] = letters[randStr.Intn(len(letters))]
		lock.Unlock()
	}
	return []byte("lotusdb-test-value-" + string(b))
}
