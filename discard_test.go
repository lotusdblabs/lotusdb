package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestDiscard_listenUpdates(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// write enough data that can trigger flush operation.
	var writeCount = 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	// delete or rewrite some keys.
	db.Put(GetKey(11), []byte("1"))
	db.Put(GetKey(4423), []byte("1"))
	db.Put(GetKey(99803), []byte("1"))
	db.Delete(GetKey(5803))
	db.Delete(GetKey(103))
	db.Put(GetKey(8888), []byte("1"))
	db.Put(GetKey(43664), []byte("1"))

	// write more to flush again.
	for i := writeCount; i <= writeCount+300000; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
}

func TestDiscard_newDiscard(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		path := filepath.Join("/tmp", "lotusdb-discard")
		os.MkdirAll(path, os.ModePerm)
		defer os.RemoveAll(path)
		dis, err := newDiscard(path, vlogDiscardName)
		assert.Nil(t, err)

		assert.Equal(t, len(dis.freeList), 341)
		assert.Equal(t, len(dis.location), 0)
	})

	t.Run("with-data", func(t *testing.T) {
		path := filepath.Join("/tmp", "lotusdb-discard")
		os.MkdirAll(path, os.ModePerm)
		defer os.RemoveAll(path)
		dis, err := newDiscard(path, vlogDiscardName)
		assert.Nil(t, err)

		for i := 1; i < 300; i = i*5 {
			dis.setTotal(uint32(i), 223)
			dis.incrDiscard(uint32(i), i * 10)
		}

		assert.Equal(t, len(dis.freeList), 337)
		assert.Equal(t, len(dis.location), 4)

		// reopen
		dis2, err := newDiscard(path, vlogDiscardName)
		assert.Nil(t, err)
		assert.Equal(t, len(dis2.freeList), 337)
		assert.Equal(t, len(dis2.location), 4)
	})
}

func TestDiscard_setToal(t *testing.T) {

}
