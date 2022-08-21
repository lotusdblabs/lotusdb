package lotusdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/flower-corp/lotusdb/logger"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	opts := DefaultOptions(t.TempDir())

	t.Run("default", func(t *testing.T) {
		newTestDB(t, opts)
	})

	t.Run("spec-dir", func(t *testing.T) {
		dir := t.TempDir()
		opts.CfOpts.IndexerDir = dir
		opts.CfOpts.ValueLogDir = dir

		newTestDB(t, opts)
	})

	t.Run("no-cf-name", func(t *testing.T) {
		opts.CfOpts.CfName = ""

		newTestDB(t, opts)
	})
}

func TestLotusDB_Put(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	type fields struct {
		db *LotusDB
	}
	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"nil-key-val", fields{db: db}, args{key: nil, value: nil}, false,
		},
		{
			"nil-key", fields{db: db}, args{key: nil, value: GetValue16B()}, false,
		},
		{
			"nil-val", fields{db: db}, args{key: GetKey(4423), value: nil}, false,
		},
		{
			"with-key-val", fields{db: db}, args{key: GetKey(990), value: GetValue16B()}, false,
		},
		{
			"with-key-big-val", fields{db: db}, args{key: GetKey(44012), value: GetValue4K()}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fields.db.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLotusDB_PutWithOptions(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	type fields struct {
		db *LotusDB
	}
	type args struct {
		key   []byte
		value []byte
		opt   *WriteOptions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"nil-options", fields{db: db}, args{key: GetKey(13), value: GetValue128B(), opt: nil}, false,
		},
		{
			"with-sync", fields{db: db}, args{key: GetKey(99832), value: GetValue128B(), opt: &WriteOptions{Sync: true}}, false,
		},
		{
			"with-disableWAL", fields{db: db}, args{key: GetKey(54221), value: GetValue128B(), opt: &WriteOptions{DisableWal: true}}, false,
		},
		{
			"with-ttl", fields{db: db}, args{key: GetKey(9901), value: GetValue128B(), opt: &WriteOptions{ExpiredAt: time.Now().Add(time.Minute).Unix()}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.fields.db
			if err := db.PutWithOptions(tt.args.key, tt.args.value, tt.args.opt); (err != nil) != tt.wantErr {
				t.Errorf("PutWithOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// We will put data until the active memtable is full and be flushed.
// Then a new active memtable will be created.
func TestLotusDB_PutUntilMemtableFlush(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	// if you change the default memtable size, change the writeCount too.
	// make sure the written data size is greater than memtable size.
	opts.CfOpts.MemtableSize = 64 << 20
	writeCount := 600000
	db := newTestDB(t, opts)

	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	// make sure all data are written.
	v1, err := db.Get(GetKey(0))
	assert.Nil(t, err)
	assert.Equal(t, len(v1), 128)
	v2, err := db.Get(GetKey(writeCount))
	assert.Nil(t, err)
	assert.Equal(t, len(v2), 128)
}

func TestLotusDB_Get(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	// write some data for getting
	for i := 0; i < 100; i++ {
		err := db.Put(GetKey(i), GetValue16B())
		if i == 43 {
			err := db.Put(GetKey(i), []byte("lotusdb"))
			assert.Nil(t, err)
		}
		assert.Nil(t, err)
	}

	type fields struct {
		db *LotusDB
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"nil", fields{db: db}, args{key: nil}, nil, false,
		},
		{
			"not-exist", fields{db: db}, args{key: GetKey(9903)}, nil, false,
		},
		{
			"get-from-memtable", fields{db: db}, args{key: GetKey(43)}, []byte{108, 111, 116, 117, 115, 100, 98}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.fields.db
			got, err := db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLotusDB_GetKeyFromIndexerAndValFromVLog(t *testing.T) {
	testGetKV(t)
}

func testGetKV(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	var writeCount = 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	keys := [][]byte{
		GetKey(0),
		GetKey(9230),
		GetKey(77842),
		GetKey(200000),
		GetKey(writeCount),
	}

	for _, tt := range keys {
		v, err := db.Get(tt)
		assert.Nil(t, err)
		assert.Equal(t, len(v), 128)
	}
}

func TestLotusDB_Delete(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	var writeCount = 100
	// write some data.
	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue16B())
		if i == 32 {
			err := db.Put(GetKey(i), []byte("lotusdb"))
			assert.Nil(t, err)
		}
		assert.Nil(t, err)
	}

	type fields struct {
		db *LotusDB
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantErr  bool
		nilValue bool
	}{
		{
			"nil", fields{db: db}, args{key: nil}, false, false,
		},
		{
			"not-existed-key", fields{db: db}, args{key: []byte("not-exist")}, false, false,
		},
		// "existed-key-1" and "existed-key-2" are suitable for deleting keys before flush.
		{
			"existed-key-1", fields{db: db}, args{key: GetKey(0)}, false, true,
		},
		{
			"existed-key-2", fields{db: db}, args{key: GetKey(writeCount)}, false, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.fields.db
			if err := db.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.nilValue {
				val, err := db.Get(tt.args.key)
				assert.Nil(t, err)
				if len(val) != 0 {
					t.Errorf("Delete() val = %v, want a nil value", val)
				}
			}
		})
	}
}

func TestLotusDB_DeleteAfterFlush(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	// write enough data that can trigger flush operation.
	var writeCount = 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		if i == 32 {
			err := db.Put(GetKey(i), []byte("lotusdb"))
			assert.Nil(t, err)
		}
		assert.Nil(t, err)
	}

	type fields struct {
		db *LotusDB
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantErr  bool
		nilValue bool
	}{
		{
			"after-flush-1", fields{db: db}, args{key: GetKey(0)}, false, true,
		},
		{
			"after-flush-2", fields{db: db}, args{key: GetKey(200000)}, false, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.fields.db
			if err := db.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.nilValue {
				val, err := db.Get(tt.args.key)
				assert.Nil(t, err)
				if len(val) != 0 {
					t.Errorf("Delete() val = %v, want a nil value", val)
				}
			}
		})
	}
}

func TestLotusDB_SyncAndClose(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	// write some data.
	var writeCount = 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	err := db.Sync()
	assert.Nil(t, err)
	err = db.Close()
	assert.Nil(t, err)
}

// write some data and reopen it.
func TestReOpenDB(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	db := newTestDB(t, opts)

	// write some data.
	var writeCount = 600000
	for i := 0; i <= writeCount; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	err := db.Close()
	assert.Nil(t, err)

	// reopen db.
	db2, err := Open(opts)
	assert.Nil(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db2.Close())
	})

	// make sure all writes are valid.
	v1, err := db2.Get(GetKey(0))
	assert.Nil(t, err)
	assert.NotNil(t, v1)

	v2, err := db2.Get(GetKey(writeCount))
	assert.Nil(t, err)
	assert.NotNil(t, v2)
}

func TestBytesFlush(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	opts.CfOpts.WalBytesFlush = 200
	db := newTestDB(t, opts)

	for i := 0; i < 10; i++ {
		err := db.Put(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
}

func newTestDB(t *testing.T, opts Options) *LotusDB {
	db, err := Open(opts)
	assert.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()

		if err := os.RemoveAll(db.opts.CfOpts.IndexerDir); err != nil {
			logger.Errorf("remove indexer path err.%v", err)
		}
		if err := os.RemoveAll(db.opts.CfOpts.ValueLogDir); err != nil {
			logger.Errorf("remove vlog path err.%v", err)
		}
	})

	return db
}

func destroyDB(db *LotusDB) {
	if db != nil {
		_ = db.Close()
		if err := os.RemoveAll(db.opts.DBPath); err != nil {
			logger.Errorf("remove db path err.%v", err)
		}
		if err := os.RemoveAll(db.opts.CfOpts.IndexerDir); err != nil {
			logger.Errorf("remove indexer path err.%v", err)
		}
		if err := os.RemoveAll(db.opts.CfOpts.ValueLogDir); err != nil {
			logger.Errorf("remove vlog path err.%v", err)
		}
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func GetValue16B() []byte {
	var str bytes.Buffer
	for i := 0; i < 16; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}

func GetValue128B() []byte {
	var str bytes.Buffer
	for i := 0; i < 128; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}

func GetValue4K() []byte {
	var str bytes.Buffer
	for i := 0; i < 4096; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}
