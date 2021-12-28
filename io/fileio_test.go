package io

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	fileName = "/tmp/test.ioselector.log"
	fsize    = 16 * 1024 * 1024
)

func TestNewFileIOSelector(t *testing.T) {
	selector, err := NewFileIOSelector(fileName, fsize)
	assert.Nil(t, err)
	assert.NotNil(t, selector)
}

func TestFileIOSelector_Write(t *testing.T) {
	selector, err := NewFileIOSelector(fileName, fsize)
	assert.Nil(t, err)
	writeSomeData(selector, t)
}

func TestFileIOSelector_Read(t *testing.T) {
	selector, err := NewFileIOSelector(fileName, fsize)
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

func TestFileIOSelector_Sync(t *testing.T) {
	selector, err := NewFileIOSelector(fileName, fsize)
	assert.Nil(t, err)
	writeSomeData(selector, t)
	err = selector.Sync()
	assert.Nil(t, err)
}

func TestFileIOSelector_Close(t *testing.T) {
	selector, err := NewFileIOSelector(fileName, fsize)
	assert.Nil(t, err)
	writeSomeData(selector, t)
	err = selector.Close()
	assert.Nil(t, err)
}

func writeSomeData(selector IOSelector, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("0"),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	var offsets []int64
	var offset int64
	for _, tt := range tests {
		offsets = append(offsets, offset)
		n, err := selector.Write(tt, offset)
		assert.Nil(t, err)
		offset += int64(n)
	}
	return offsets
}
