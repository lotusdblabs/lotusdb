package io

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	mmapName = "/tmp/test.mmapselector.log"
	msize    = 16 * 1024 * 1024
)

func TestNewMMapSelector(t *testing.T) {
	selector, err := NewMMapSelector(mmapName, msize)
	assert.Nil(t, err)
	assert.NotNil(t, selector)
}

func TestMMapSelector_Write(t *testing.T) {
	selector, err := NewMMapSelector(mmapName, msize)
	assert.Nil(t, err)
	writeSomeData(selector, t)
}

func TestMMapSelector_Read(t *testing.T) {
	selector, err := NewMMapSelector(mmapName, msize)
	assert.Nil(t, err)
	offsets := writeSomeData(selector, t)

	tests := [][]byte{
		[]byte(""),
		[]byte("0"),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	read := func(offset int64, n int, expected []byte) {
		buf := make([]byte, n)
		_, err := selector.Read(buf, offset)
		assert.Nil(t, err)
		assert.Equal(t, expected, buf)
	}

	n := []int{0, 1, 1, 7}
	for i, off := range offsets {
		read(off, n[i], tests[i])
	}

}

func TestMMapSelector_Sync(t *testing.T) {
	selector, err := NewMMapSelector(mmapName, msize)
	assert.Nil(t, err)
	writeSomeData(selector, t)
	err = selector.Sync()
	assert.Nil(t, err)
}

func TestMMapSelector_Close(t *testing.T) {
	selector, err := NewMMapSelector(mmapName, msize)
	assert.Nil(t, err)
	writeSomeData(selector, t)
	err = selector.Close()
	assert.Nil(t, err)
}
