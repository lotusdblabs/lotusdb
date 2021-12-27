package io

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMMapSelector(t *testing.T) {
	selector, err := NewMMapSelector(fileName, fsize)
	assert.Nil(t, err)
	assert.NotNil(t, selector)
}
