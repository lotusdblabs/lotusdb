package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestKey(int64(i))))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(RandomValue(10)))
	}
}
