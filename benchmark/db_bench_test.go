package benchmark

import (
	"fmt"
	"github.com/flower-corp/lotusdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Simple Benchmark for LotusDB

var db *lotusdb.LotusDB

func init() {
	var err error
	options := lotusdb.DefaultOptions("/tmp/lotusdb")
	db, err = lotusdb.Open(options)
	if err != nil {
		panic(fmt.Sprintf("open lotusdb err.%+v", err))
	}
}

func initData(b *testing.B, db *lotusdb.LotusDB) {
	for i := 0; i < 500000; i++ {
		err := db.Put(getKey(i), getValue128B())
		assert.Nil(b, err)
	}

	for i := 500000; i < 1000000; i++ {
		err := db.Put(getKey(i), getValue4K())
		assert.Nil(b, err)
	}
}

func BenchmarkLotusDB_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkLotusDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(getKey(i))
		assert.Nil(b, err)
	}
}
