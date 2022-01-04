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
