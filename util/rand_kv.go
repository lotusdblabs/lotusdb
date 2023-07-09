package util

import "math/rand"

const (
	DefaultKeyNum = 10000
)

func GenerateKeys(count int, minL int, maxL int) [][]byte {
	keys := make([][]byte, 0, count)
	recordK := make(map[string]struct{}, count)
	for len(keys) < count {
		k := randKey(minL, maxL)
		if _, ok := recordK[k]; ok {
			continue
		}
		recordK[k] = struct{}{}
		keys = append(keys, []byte(k))
	}
	return keys
}

// randKey random key size with min and max
func randKey(minL int, maxL int) string {
	n := rand.Intn(maxL-minL+1) + minL
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		buf[i] = byte(rand.Intn(95) + 32)
	}
	return string(buf)
}

// RandValue random value size with src、min and max
// rnd for random more real
func RandValue(rnd *rand.Rand, src []byte, minS int, maxS int) []byte {
	n := rnd.Intn(maxS-minS+1) + minS
	return src[:n]
}

// Shuffle shuffle keys
func Shuffle(a [][]byte) {
	for i := len(a) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}
