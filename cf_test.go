package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLotusDB_OpenColumnFamily(t *testing.T) {
	opts := DefaultOptions("/tmp/lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer db.Close()

	cfopts := DefaultColumnFamilyOptions("new-two")
	cfopts.IndexerDir = "/tmp/indexer/"
	columnFamily, err := db.OpenColumnFamily(cfopts)
	assert.Nil(t, err)
	t.Log(columnFamily)
}
