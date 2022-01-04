package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Benchmark for LotusDB

func BenchmarkLotusDB_Put(b *testing.B) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(b, err)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(GetKey(i), GetValue())
		assert.Nil(b, err)
	}
}

func initData(b *testing.B, db *LotusDB) {
	for i := 0; i < 500000; i++ {
		err := db.Put(GetKey(i), GetValue128())
		assert.Nil(b, err)
	}

	for i := 500000; i < 1000000; i++ {
		err := db.Put(GetKey(i), GetValue())
		assert.Nil(b, err)
	}
}

func BenchmarkLotusDB_Get(b *testing.B) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(b, err)

	//initData(b, db)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(GetKey(i))
		assert.Nil(b, err)
	}
}
