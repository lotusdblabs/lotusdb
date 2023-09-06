package benchmark

import (
	"github.com/lotusdblabs/lotusdb/v2"
	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var db *lotusdb.DB

func openDB() func() {
	options := lotusdb.DefaultOptions
	options.DirPath = "/tmp/lotusdb-bench"

	var err error
	db, err = lotusdb.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPut(b *testing.B) {
	destroy := openDB()
	defer destroy()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomValue(1024), lotusdb.DefaultWriteOptions)
		assert.Nil(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	destroy := openDB()
	defer destroy()
	for i := 0; i < 1000000; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomValue(128), lotusdb.DefaultWriteOptions)
		assert.Nil(b, err)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		val, err := db.Get(util.GetTestKey(i))
		if err == nil {
			assert.NotNil(b, val)
		} else if err != lotusdb.ErrKeyNotFound {
			b.Error(err)
		}
	}
}
