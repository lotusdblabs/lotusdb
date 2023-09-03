package util

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "db-test-DirSize")
	assert.Nil(t, err)

	defer os.RemoveAll(dirPath)

	err = os.MkdirAll(dirPath, os.ModePerm)
	assert.Nil(t, err)
	file, err := os.Create(fmt.Sprintf("%s/test.txt", dirPath))
	assert.Nil(t, err)
	byteSilces := []byte("test")
	_, err = file.Write(byteSilces)
	assert.Nil(t, err)
	err = file.Close()
	assert.Nil(t, err)

	t.Run("test DirSize", func(t *testing.T) {
		size, err := DirSize(dirPath)
		assert.Nil(t, err)
		assert.Greater(t, size, int64(0))
	})
}
