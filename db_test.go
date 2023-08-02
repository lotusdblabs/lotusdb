package lotusdb

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gofrs/flock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_Open(t *testing.T) {
	// Save the original function
	t.Run("Valid options", func(t *testing.T) {
		options := DefaultOptions
		options.DirPath = t.TempDir()
		defer os.RemoveAll(options.DirPath)
		db, err := Open(options)
		require.NoError(t, err, "Open should not return an error")
		require.NotNil(t, db, "DB should not be nil")
		// Ensure that the properties of the DB are initialized correctly.
		assert.NotNil(t, db.activeMem, "DB activeMem should not be nil")
		assert.Equal(t, db.activeMem.options.memSize, options.MemtableSize, "DB activeMem options.memSize should be equal to options.MemtableSize")
		assert.Len(t, db.immuMems, 0, "DB immuMems should be empty")
		assert.NotNil(t, db.index, "DB index should not be nil")
		assert.NotNil(t, db.vlog, "DB vlog should not be nil")
		assert.NotNil(t, db.fileLock, "DB fileLock should not be nil")
		assert.NotNil(t, db.flushChan, "DB flushChan should not be nil")

		assert.NoError(t, db.Close(), "Close should not return an error")
	})
	t.Run("Invalid options - no directory path", func(t *testing.T) {
		options := DefaultOptions
		options.DirPath = ""
		_, err := Open(options)
		require.Error(t, err, "Open should return an error")
	})
	t.Run("Multiple processes accessing the same database directory", func(t *testing.T) {
		dirPath := t.TempDir()
		os.MkdirAll(dirPath, os.ModePerm)
		defer os.RemoveAll(dirPath)
		fileLock := flock.New(filepath.Join(dirPath, fileLockName))
		_, err := fileLock.TryLock()
		require.NoError(t, err)
		options := DefaultOptions
		options.DirPath = dirPath
		_, err = Open(options)
		require.Error(t, err, "Open should return an error")
		// Clean up
		assert.NoError(t, fileLock.Unlock())
	})
	t.Run("OpenAllMemtables_Error", func(t *testing.T) {
		// Mock openIndex to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyFunc(openAllMemtables, func(_ Options) ([]*memtable, error) {
			return nil, fmt.Errorf("mock error")
		})

		// Ensure the patch is reverted at the end of the test
		defer patches.Reset()

		options := DefaultOptions
		_, err := Open(options)
		require.Error(t, err, "Open should return an error")
	})

	t.Run("OpenIndex error", func(t *testing.T) {
		// Mock openIndex to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyFunc(openIndex, func(_ indexOptions) (Index, error) {
			return nil, fmt.Errorf("mock error")
		})
		patches.ApplyFunc(openIndexBoltDB, func(_ indexOptions) (*BPTree, error) {
			return nil, fmt.Errorf("mock error")
		})
		defer patches.Reset()

		options := DefaultOptions
		_, err := Open(options)
		require.Error(t, err, "Open should return an error")
	})

	t.Run("OpenValueLog error", func(t *testing.T) {
		// Mock openValueLog to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyFunc(openValueLog, func(_ valueLogOptions) (*valueLog, error) {
			return nil, fmt.Errorf("mock error")
		})
		defer patches.Reset()

		options := DefaultOptions
		_, err := Open(options)
		require.Error(t, err, "Open should return an error")
	})

}

func TestDB_Close(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	require.NoError(t, err, "Open should not return an error")
	require.NotNil(t, db, "DB should not be nil")
	t.Run("Close all memtables error", func(t *testing.T) {
		// Mock the close method in activeMem to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyPrivateMethod(reflect.TypeOf(db.activeMem), "close", func(_ *memtable) error {
			return fmt.Errorf("mock error")
		})
		patches.ApplyMethod(reflect.TypeOf(db.activeMem.wal), "Close", func() error {
			return fmt.Errorf("mock error")
		})
		defer patches.Reset()
		err := db.Close()
		require.Error(t, err, "Close should return an error")
	})

	t.Run("Close active memtable error", func(t *testing.T) {
		// Mock the close method in activeMem to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyPrivateMethod(reflect.TypeOf(db.activeMem), "close", func(_ *memtable) error {
			return fmt.Errorf("mock error")
		})
		patches.ApplyMethod(reflect.TypeOf(db.activeMem.wal), "Close", func() error {
			return fmt.Errorf("mock error")
		})
		defer patches.Reset()

		err := db.Close()
		require.Error(t, err, "Close should return an error")
	})

	t.Run("Close index error", func(t *testing.T) {
		// Mock the close method in activeMem to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyPrivateMethod(reflect.TypeOf(db.index), "Close", func() error {
			return fmt.Errorf("mock error")
		})
		defer patches.Reset()

		err := db.Close()
		require.Error(t, err, "Close should return an error")
	})

	t.Run("Success", func(t *testing.T) {
		err := db.Close()
		require.NoError(t, err, "Close should not return an error")
	})
}

func TestDB_Sync(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
	t.Run("Sync all memtables error", func(t *testing.T) {
		// Mock the sync method in activeMem to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyPrivateMethod(reflect.TypeOf(db.activeMem), "sync", func(_ *memtable) error {
			return fmt.Errorf("mock error")
		})
		patches.ApplyMethod(reflect.TypeOf(db.activeMem.wal), "Sync", func() error {
			return fmt.Errorf("mock error")
		})
		defer patches.Reset()
		err := db.Sync()
		require.Error(t, err, "Sync should return an error")
	})

	t.Run("Sync active memtable error", func(t *testing.T) {
		// Mock the sync method in activeMem to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyPrivateMethod(reflect.TypeOf(db.activeMem), "sync", func(_ *memtable) error {
			return fmt.Errorf("mock error")
		})
		patches.ApplyMethod(reflect.TypeOf(db.activeMem.wal), "Sync", func() error {
			return fmt.Errorf("mock error")
		})
		defer patches.Reset()

		err := db.Sync()
		require.Error(t, err, "Sync should return an error")
	})

	t.Run("Sync index error", func(t *testing.T) {
		// Mock the sync method in activeMem to return an error
		patches := gomonkey.NewPatches()
		patches.ApplyPrivateMethod(reflect.TypeOf(db.index), "Sync", func() error {
			return fmt.Errorf("mock error")
		})
		defer patches.Reset()

		err := db.Sync()
		require.Error(t, err, "Sync should return an error")
	})

	t.Run("Success", func(t *testing.T) {
		err := db.Sync()
		require.NoError(t, err, "Sync should not return an error")
	})
}

// test Put and Get function
func TestDB_Put_Get(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.NoError(t, err, "Open should not return an error")
	assert.NotNil(t, db, "db should not be nil")

	t.Run("Put and Get", func(t *testing.T) {
		k, v := []byte("Hello"), []byte("World")
		err := db.Put(k, v, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		assert.NoError(t, err, "Put should not return an error")

		val, err := db.Get(k)
		assert.NoError(t, err, "Get should not return an error")
		assert.Equal(t, v, val, "expected value is World")
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		_, err := db.Get([]byte("key"))
		assert.EqualError(t, err, ErrKeyNotFound.Error(), "expected key not found")
	})

	err = db.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// test Delete and Exist function
func TestDB_Delete_Exist(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.NoError(t, err, "Open should not return an error")
	assert.NotNil(t, db, "db should not be nil")

	writeOptions := &WriteOptions{
		Sync:       true,
		DisableWal: false,
	}

	k, v := []byte("Lumia"), []byte("Qian")
	err = db.Put(k, v, writeOptions)
	assert.NoError(t, err, "Put should not return an error")

	t.Run("Exist", func(t *testing.T) {
		isExist, err := db.Exist(k)
		assert.NoError(t, err, "Exist should not return an error")
		assert.Equal(t, true, isExist, "expected isExist is true")
	})

	t.Run("Delete", func(t *testing.T) {
		err = db.Delete(k, writeOptions)
		assert.NoError(t, err, "Delete should not return an error")
	})

	t.Run("Exist after Delete", func(t *testing.T) {
		isExist, err := db.Exist(k)
		assert.NoError(t, err, "Exist should not return an error")
		assert.Equal(t, false, isExist, "expected isExist is false")
	})

	t.Run("Get after Delete", func(t *testing.T) {
		val, err := db.Get(k)
		assert.EqualError(t, err, ErrKeyNotFound.Error(), "expected key not found")
		assert.Nil(t, val, "expected val nil")
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		err = db.Delete([]byte("Hello"), writeOptions)
		assert.NoError(t, err, "Delete should not return an error")
	})

	err = db.Close()
	assert.NoError(t, err, "Close should not return an error")
}
