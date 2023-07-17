package lotusdb

import (
	"github.com/lotusdblabs/lotusdb/v2/util"
	"testing"
)

func TestOpenDB(t *testing.T) {
	options := DefaultOptions
	options.DirPath = "/tmp/lotusdb"
	db, err := Open(options)
	t.Log(db, err)

	for i := 0; i < 1000000; i++ {
		err := db.Put(util.RandomValue(10), util.RandomValue(100), nil)
		if err != nil {
			panic(err)
		}
	}
}
