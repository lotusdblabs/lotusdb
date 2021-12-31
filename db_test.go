package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpen(t *testing.T) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(t, err)
	defer db.Close()

	//err = db.Put([]byte("key-1"), []byte("LotusDB"))
	//assert.Nil(t, err)

	//
	//[]byte("key-1"), []byte("val-1")
	key := []byte("key-1")

	v, err := db.Get(key)
	t.Log(string(v))
}
