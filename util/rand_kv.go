package util

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	lock       = sync.Mutex{}
	randStr    = rand.New(rand.NewSource(time.Now().Unix()))
	letters    = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	lettersLen = len(letters)
)

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("lotusdb-test-key-%09d", i))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		lock.Lock()
		b[i] = letters[randStr.Intn(lettersLen)]
		lock.Unlock()
	}
	return []byte("lotusdb-test-value-" + string(b))
}
