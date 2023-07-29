package lotusdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func Test_openIndexBoltDB(t *testing.T) {
	tests := []struct {
		name    string
		options indexOptions
		want    *BPTree
		wantErr bool
	}{
		{"normal",
			indexOptions{
				indexType:       indexBoltDB,
				dirPath:         filepath.Join("/tmp", "bptree-open"),
				partitionNum:    3,
				hashKeyFunction: xxhash.Sum64,
			},
			&BPTree{
				options: indexOptions{
					indexType:       indexBoltDB,
					dirPath:         filepath.Join("/tmp", "bptree-open"),
					partitionNum:    3,
					hashKeyFunction: xxhash.Sum64,
				},
				trees: make([]*bbolt.DB, 3),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.MkdirAll(tt.options.dirPath, os.ModePerm)
			defer func() {
				_ = os.RemoveAll(tt.options.dirPath)
			}()
			got, err := openIndexBoltDB(tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("openIndexBoltDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want.options.indexType, got.options.indexType)
			assert.Equal(t, tt.want.options.dirPath, got.options.dirPath)
			assert.Equal(t, tt.want.options.partitionNum, got.options.partitionNum)
			assert.Equal(t, len(tt.want.trees), len(got.trees))
		})

	}
}

func TestBPTree_Get(t *testing.T) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join("/tmp", "bptree-get"),
		partitionNum:    3,
		hashKeyFunction: xxhash.Sum64,
	}

	_ = os.MkdirAll(options.dirPath, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(bt.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})
	err = bt.PutBatch(keyPositions)
	assert.Nil(t, err)

	tests := []struct {
		name    string
		key     []byte
		exist   bool
		wantErr bool
	}{
		{"nil", nil, false, false},
		{"not-exist", []byte("not-exist"), false, false},
		{"exist", []byte("exist"), true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bt.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("BPTree.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Log(got)
			assert.Equal(t, got != nil, tt.exist)
		})
	}
}

func TestBPTree_PutBatch(t *testing.T) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join("/tmp", "bptree-putBatch"),
		partitionNum:    3,
		hashKeyFunction: xxhash.Sum64,
	}

	_ = os.MkdirAll(options.dirPath, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)

	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       nil,
		partition: 0,
		position:  &wal.ChunkPosition{},
	})

	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("normal"),
		partition: uint32(bt.getKeyPartition([]byte("normal"))),
		position:  &wal.ChunkPosition{},
	})

	tests := []struct {
		name      string
		positions []*KeyPosition
		wantErr   bool
	}{
		{"empty", keyPositions[:0], false},
		{"nil-key", keyPositions[:1], true},
		{"normal", keyPositions[1:2], false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := bt.PutBatch(tt.positions); (err != nil) != tt.wantErr {
				t.Errorf("BPTree.PutBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBPTree_DeleteBatch(t *testing.T) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join("/tmp", "bptree-deleteBatch"),
		partitionNum:    3,
		hashKeyFunction: xxhash.Sum64,
	}

	_ = os.MkdirAll(options.dirPath, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keys [][]byte
	keys = append(keys, nil, []byte("not-exist"), []byte("exist"))
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(bt.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})

	err = bt.PutBatch(keyPositions)
	assert.Nil(t, err)

	tests := []struct {
		name    string
		keys    [][]byte
		wantErr bool
	}{
		{"nil", keys[:1], false},
		{"no-exist", keys[1:2], false},
		{"exist", keys[2:3], false},
		{"all", keys, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := bt.DeleteBatch(tt.keys); (err != nil) != tt.wantErr {
				t.Errorf("BPTree.DeleteBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBPTree_Close(t *testing.T) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join("/tmp", "bptree-close"),
		partitionNum:    3,
		hashKeyFunction: xxhash.Sum64,
	}

	_ = os.MkdirAll(options.dirPath, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)

	err = bt.Close()
	assert.Nil(t, err)
}

func TestBPTree_getKeyPartition(t *testing.T) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join("/tmp", "bptree-getKeyPartition"),
		partitionNum:    3,
		hashKeyFunction: xxhash.Sum64,
	}

	_ = os.MkdirAll(options.dirPath, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keys [][]byte
	for i := 0; i < 3; i++ {
		keys = append(keys, util.GetTestKey(10))
	}
	tests := []struct {
		name string
		key  []byte
		want int
	}{
		{"t0", keys[0], int(xxhash.Sum64(keys[0]) % uint64(bt.options.partitionNum))},
		{"t1", keys[1], int(xxhash.Sum64(keys[1]) % uint64(bt.options.partitionNum))},
		{"t2", keys[2], int(xxhash.Sum64(keys[2]) % uint64(bt.options.partitionNum))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bt.getKeyPartition(tt.key)
			if got != tt.want {
				t.Errorf("BPTree.getKeyPartition() = %v, want %v", got, tt.want)
			}
			assert.True(t, got < bt.options.partitionNum)
		})
	}
}

func TestBPTree_Sync(t *testing.T) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join("/tmp", "bptree-sync"),
		partitionNum:    3,
		hashKeyFunction: xxhash.Sum64,
	}

	_ = os.MkdirAll(options.dirPath, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	err = bt.Sync()
	assert.Nil(t, err)
}