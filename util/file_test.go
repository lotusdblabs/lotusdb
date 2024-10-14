package util

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirSize(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "db-test-DirSize")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(dirPath)
	}()

	err = os.MkdirAll(dirPath, os.ModePerm)
	require.NoError(t, err)
	file, err := os.Create(fmt.Sprintf("%s/test.txt", dirPath))
	require.NoError(t, err)
	byteSilces := []byte("test")
	_, err = file.Write(byteSilces)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	t.Run("test DirSize", func(t *testing.T) {
		size, errDirSize := DirSize(dirPath)
		require.NoError(t, errDirSize)
		assert.Positive(t, size, int64(0))
	})
}
