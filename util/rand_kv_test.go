package util

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateKeys(t *testing.T) {
	keys := GenerateKeys(100000, 16, 256)
	for _, k := range keys {
		assert.NotNil(t, string(k))
	}
}

func BenchmarkGenerateKeys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateKeys(100000, 16, 256)
	}
}

func TestRandValue(t *testing.T) {
	maxV, minV := 1024, 256
	valSrc := make([]byte, maxV)
	if _, err := rand.Read(valSrc); err != nil {
		t.Fatal(err)
	}
	rnd := rand.New(rand.NewSource(int64(rand.Uint64())))
	for i := 0; i < 100; i++ {
		v := RandValue(rnd, valSrc, minV, maxV)
		assert.NotNil(t, v)
	}
}

func BenchmarkRandValue(b *testing.B) {
	maxV, minV := 1024, 256
	valSrc := make([]byte, maxV)
	if _, err := rand.Read(valSrc); err != nil {
		b.Fatal(err)
	}
	rnd := rand.New(rand.NewSource(int64(rand.Uint64())))
	for i := 0; i < b.N; i++ {
		RandValue(rnd, valSrc, minV, maxV)
	}
}
