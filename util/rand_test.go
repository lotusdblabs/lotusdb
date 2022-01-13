package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFastrand(t *testing.T) {
	r := Fastrand()
	assert.NotZero(t, r)
}
