package lotusdb

import (
	"bytes"
	"fmt"
	"github.com/flower-corp/lotusdb/logger"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	opendb := func(opts Options) {
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
	}

	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	t.Run("default", func(t *testing.T) {
		opendb(opts)
	})

	t.Run("spec-dir", func(t *testing.T) {
		dir := "/tmp" + separator + "new-dir"
		opts.CfOpts.IndexerDir = dir
		opts.CfOpts.ValueLogDir = dir
		opendb(opts)
	})
}

func TestLotusDB_Put(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

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
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

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

func TestLotusDB_Get(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

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

func destroyDB(db *LotusDB) {
	if db != nil {
		if err := os.RemoveAll(db.opts.DBPath); err != nil {
			logger.Errorf("remove db path err.%v", err)
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
