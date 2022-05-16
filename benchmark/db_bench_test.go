package benchmark

import (
	"fmt"
	"testing"

	"github.com/flower-corp/lotusdb"
	"github.com/stretchr/testify/assert"
)

// Simple Benchmark for LotusDB

var db *lotusdb.LotusDB

func init() {
	var err error
	options := lotusdb.DefaultOptions("/tmp/lotusdb2benchmark")
	db, err = lotusdb.Open(options)
	if err != nil {
		panic(fmt.Sprintf("open lotusdb err.%+v", err))
	}
	initData(db)
}

func initData(db *lotusdb.LotusDB) {
	for i := 0; i < 500000; i++ {
		err := db.Put(getKey(i), getValue512B())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLotusDB_Put128B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkLotusDB_Put512B(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(getKey(i), getValue512B())
		assert.Nil(b, err)
	}
}

func BenchmarkLotusDB_Put_4k(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(getKey(i), getValue4K())
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

func BenchmarkLotusDB_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(getKey(i))
		assert.Nil(b, err)
	}
}

func TestKVData(t *testing.T) {
	key := getKey(0)
	assert.NotNil(t, key)

	v1 := getValue128B()
	assert.NotNil(t, v1)

	v2 := getValue4K()
	assert.NotNil(t, v2)
}
