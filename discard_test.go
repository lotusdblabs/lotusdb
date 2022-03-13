package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
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

	// read disard file
	dir := filepath.Join(opts.DBPath, DefaultColumnFamilyName)
	discard, _ := newDiscard(dir, vlogDiscardName)
	fid, err := discard.maxDiscardFid()
	assert.Nil(t, err)
	assert.Equal(t, fid, uint32(0))
}

func TestDiscard_incrTotalAndDiscard(t *testing.T) {
	dir, _ := ioutil.TempDir("", "lotusdb-discard-test")
	discard, err := newDiscard(dir, vlogDiscardName)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	type args struct {
		fid uint32
	}
	tests := []struct {
		name string
		d    *Discard
		args args
	}{
		{
			"total-fid-0", discard, args{fid: 0},
		},
		{
			"discard-fid-0", discard, args{fid: 0},
		},
		{
			"total-fid-1", discard, args{fid: 1},
		},
		{
			"discard-fid-1", discard, args{fid: 1},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if i%2 == 0 {
				tt.d.incrTotal(tt.args.fid)
			} else {
				tt.d.incrDiscard(tt.args.fid)
			}
		})
	}
}

func TestDiscard_clear(t *testing.T) {
	dir, _ := ioutil.TempDir("", "lotusdb-discard-test")
	d, err := newDiscard(dir, vlogDiscardName)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	for i := 0; i < 1000; i++ {
		for j := 1; j < 1000; j = j * 11 {
			if i%2 == 0 {
				d.incrTotal(uint32(j))
			} else {
				d.incrDiscard(uint32(j))
			}
		}
	}
	d.clear(1)
	d.clear(11)
}

func BenchmarkDiscard_incrTotalAndDiscard(b *testing.B) {
	dir, _ := ioutil.TempDir("", "lotusdb-discard-test")
	discard, err := newDiscard(dir, vlogDiscardName)
	assert.Nil(b, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			discard.incr(uint32(i%255), true, 1)
		} else {
			discard.incr(uint32(i%255), false, 1)
		}
	}
}
