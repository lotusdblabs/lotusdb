package lotusdb

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

func TestOpenHashTable(t *testing.T) {
	tests := []struct {
		name    string
		options indexOptions
		want    *HashTable
		wantErr bool
	}{
		{"normal_1",
			indexOptions{
				indexType:       Hash,
				dirPath:         filepath.Join(os.TempDir(), "hashtable-open-1"),
				partitionNum:    1,
				hashKeyFunction: xxhash.Sum64,
			},
			&HashTable{
				options: indexOptions{
					indexType:       Hash,
					dirPath:         filepath.Join(os.TempDir(), "hashtable-open-1"),
					partitionNum:    1,
					hashKeyFunction: xxhash.Sum64,
				},
				tables: make([]*diskhash.Table, 1),
			},
			false,
		},
		{"normal_3",
			indexOptions{
				indexType:       Hash,
				dirPath:         filepath.Join(os.TempDir(), "hashtable-open-3"),
				partitionNum:    3,
				hashKeyFunction: xxhash.Sum64,
			},
			&HashTable{
				options: indexOptions{
					indexType:       Hash,
					dirPath:         filepath.Join(os.TempDir(), "hashtable-open-3"),
					partitionNum:    3,
					hashKeyFunction: xxhash.Sum64,
				},
				tables: make([]*diskhash.Table, 3),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.MkdirAll(tt.options.dirPath, os.ModePerm)
			assert.Nil(t, err)
			defer func() {
				_ = os.RemoveAll(tt.options.dirPath)
			}()
			got, err := openHashIndex(tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("openHashTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want.options.indexType, got.options.indexType)
			assert.Equal(t, tt.want.options.dirPath, got.options.dirPath)
			assert.Equal(t, tt.want.options.partitionNum, got.options.partitionNum)
			assert.Equal(t, len(tt.want.tables), len(got.tables))
		})

	}
}

func TestHashTable_PutBatch(t *testing.T) {
	testhashtablePutbatch(t, 1)
	testhashtablePutbatch(t, 3)
}

func testhashtablePutbatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-putBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	assert.Nil(t, err)

	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       nil,
		partition: 0,
		position:  &wal.ChunkPosition{},
	})

	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("normal"),
		partition: uint32(ht.options.getKeyPartition([]byte("normal"))),
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
			if err := ht.PutBatch(tt.positions); (err != nil) != tt.wantErr {
				t.Errorf("HashTable.PutBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashTable_Get(t *testing.T) {
	testhashtableGet(t, 1)
	testhashtableGet(t, 3)

}

func testhashtableGet(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-get-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	assert.Nil(t, err)
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(ht.options.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})
	err = ht.PutBatch(keyPositions)
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
			got, err := ht.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("HashTable.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, got != nil, tt.exist)
		})
	}
}

func TestHashTable_DeleteBatch(t *testing.T) {
	testhashtableDeletebatch(t, 1)
	testhashtableDeletebatch(t, 3)
}

func testhashtableDeletebatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-deleteBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	assert.Nil(t, err)
	var keys [][]byte
	keys = append(keys, nil, []byte("not-exist"), []byte("exist"))
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(ht.options.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})

	err = ht.PutBatch(keyPositions)
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
			if err := ht.DeleteBatch(tt.keys); (err != nil) != tt.wantErr {
				t.Errorf("HashTable.DeleteBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashTable_Close(t *testing.T) {
	testhashtableClose(t, 1)
	testhashtableClose(t, 3)
}

func testhashtableClose(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-close-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	assert.Nil(t, err)

	err = ht.Close()
	assert.Nil(t, err)
}

func TestHashTable_Sync(t *testing.T) {
	testhashtableSync(t, 1)
	testhashtableSync(t, 3)
}

func testhashtableSync(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-sync-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	assert.Nil(t, err)

	err = ht.Sync()
	assert.Nil(t, err)
}
