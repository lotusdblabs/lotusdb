package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestLotusDB_OpenColumnFamily(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	opencf := func(opts ColumnFamilyOptions) {
		cf, err := db.OpenColumnFamily(opts)
		assert.Nil(t, err)
		assert.NotNil(t, cf)
	}

	t.Run("default", func(t *testing.T) {
		cfopt := DefaultColumnFamilyOptions("cf-1")
		opencf(cfopt)
	})

	t.Run("spec-dir", func(t *testing.T) {
		cfopt := DefaultColumnFamilyOptions("cf-1")
		dir, _ := ioutil.TempDir("", "lotusdb")
		defer func() {
			_ = os.RemoveAll(dir)
		}()
		cfopt.DirPath = dir
		opencf(cfopt)
	})

	t.Run("spec-val-dir", func(t *testing.T) {
		cfopt := DefaultColumnFamilyOptions("cf-1")
		dir, _ := ioutil.TempDir("", "lotusdb")
		valDir, _ := ioutil.TempDir("", "lotus-val")
		defer func() {
			_ = os.RemoveAll(dir)
			_ = os.RemoveAll(valDir)
		}()

		cfopt.DirPath = dir
		cfopt.ValueLogDir = valDir
		opencf(cfopt)
	})
}

func TestColumnFamily_Put(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	cf, err := db.OpenColumnFamily(DefaultColumnFamilyOptions("cf_default"))
	assert.Nil(t, err)

	type fields struct {
		cf *ColumnFamily
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
			"nil-key-val", fields{cf: cf}, args{key: nil, value: nil}, false,
		},
		{
			"nil-key", fields{cf: cf}, args{key: nil, value: GetValue16B()}, false,
		},
		{
			"nil-val", fields{cf: cf}, args{key: GetKey(4423), value: nil}, false,
		},
		{
			"with-key-val", fields{cf: cf}, args{key: GetKey(990), value: GetValue16B()}, false,
		},
		{
			"with-key-big-val", fields{cf: cf}, args{key: GetKey(44012), value: GetValue4K()}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cf := tt.fields.cf
			if err := cf.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestColumnFamily_PutWithOptions(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	cf, err := db.OpenColumnFamily(DefaultColumnFamilyOptions("cf_default"))
	assert.Nil(t, err)

	type fields struct {
		cf *ColumnFamily
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
			"nil-options", fields{cf: cf}, args{key: GetKey(13), value: GetValue128B(), opt: nil}, false,
		},
		{
			"with-sync", fields{cf: cf}, args{key: GetKey(99832), value: GetValue128B(), opt: &WriteOptions{Sync: true}}, false,
		},
		{
			"with-disableWAL", fields{cf: cf}, args{key: GetKey(54221), value: GetValue128B(), opt: &WriteOptions{DisableWal: true}}, false,
		},
		{
			"with-ttl", fields{cf: cf}, args{key: GetKey(9901), value: GetValue128B(), opt: &WriteOptions{ExpiredAt: time.Now().Add(time.Minute).Unix()}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cf := tt.fields.cf
			if err := cf.PutWithOptions(tt.args.key, tt.args.value, tt.args.opt); (err != nil) != tt.wantErr {
				t.Errorf("PutWithOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestColumnFamily_Get(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	cf, err := db.OpenColumnFamily(DefaultColumnFamilyOptions("cf_default"))
	assert.Nil(t, err)

	// write some data for getting
	for i := 0; i < 100; i++ {
		err := db.Put(GetKey(i), GetValue16B())
		if i == 43 {
			err := cf.Put(GetKey(i), []byte("lotusdb"))
			assert.Nil(t, err)
		}
		assert.Nil(t, err)
	}

	type fields struct {
		cf *ColumnFamily
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
			"nil", fields{cf: cf}, args{key: nil}, nil, false,
		},
		{
			"not-exist", fields{cf: cf}, args{key: GetKey(9903)}, nil, false,
		},
		{
			"get-from-memtable", fields{cf: cf}, args{key: GetKey(43)}, []byte{108, 111, 116, 117, 115, 100, 98}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cf := tt.fields.cf
			got, err := cf.Get(tt.args.key)
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
