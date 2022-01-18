package index

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	path, err := ioutil.TempDir("", "indexer")
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	type args struct {
		opt *BPTreeOptions
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"normal", args{opt: &BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test-1", BucketName: []byte("test-1"), BatchSize: 1000}}, false,
		},
		{
			"no-dir-path", args{opt: &BPTreeOptions{DirPath: "", IndexType: BptreeBoltDB, ColumnFamilyName: "test-1", BucketName: []byte("test-1"), BatchSize: 1000}}, true,
		},
		{
			"no-cf-name", args{opt: &BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "", BucketName: []byte("test-1"), BatchSize: 1000}}, true,
		},
		{
			"no-bucket-name", args{opt: &BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test-1", BucketName: nil, BatchSize: 1000}}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewBPTree(tt.args.opt)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBPTree() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				t.Errorf("NewBPTree() want a bptree, but got a nil value")
			}
		})
	}
}

func TestBPTree_Put(t *testing.T) {
	type fields struct {
		bptree *BPTree
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
		{},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.fields.bptree
			if err := b.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
