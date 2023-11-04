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
		options Options
		want    *HashTable
		wantErr bool
	}{
		{"normal_1",
			Options{
				IndexType:       Hash,
				DirPath:         filepath.Join(os.TempDir(), "hashtable-open-1"),
				PartitionNum:    1,
				KeyHashFunction: xxhash.Sum64,
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
			Options{
				IndexType:       Hash,
				DirPath:         filepath.Join(os.TempDir(), "hashtable-open-3"),
				PartitionNum:    3,
				KeyHashFunction: xxhash.Sum64,
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
			defer func() {
				_ = os.RemoveAll(tt.options.DirPath)
			}()
			db, err := Open(tt.options)
			assert.Nil(t, err)
			got := db.index.(*HashTable)
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
	testHashTablePutBatch(t, 1)
	testHashTablePutBatch(t, 3)
}

func testHashTablePutBatch(t *testing.T, partitionNum int) {
	options := Options{
		IndexType:       Hash,
		DirPath:         filepath.Join(os.TempDir(), "hashtable-putBatch-"+strconv.Itoa(partitionNum)),
		PartitionNum:    partitionNum,
		KeyHashFunction: xxhash.Sum64,
	}

	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := Open(options)
	assert.Nil(t, err)
	ht := db.index.(*HashTable)

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
			matchKeys := make([]diskhash.MatchKeyFunc, len(tt.positions))
			for i := range matchKeys {
				matchKeys[i] = MatchKeyFunc(db, tt.positions[i].key, nil, nil)
			}
			if err := ht.PutBatch(tt.positions, matchKeys...); (err != nil) != tt.wantErr {
				t.Errorf("HashTable.PutBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashTable_Get(t *testing.T) {
	testHashTableGet(t, 1)
	testHashTableGet(t, 3)
}

func testHashTableGet(t *testing.T, partitionNum int) {
	options := Options{
		IndexType:       Hash,
		DirPath:         filepath.Join(os.TempDir(), "hashtable-get-"+strconv.Itoa(partitionNum)),
		PartitionNum:    partitionNum,
		KeyHashFunction: xxhash.Sum64,
	}
	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()
	db, err := Open(options)
	assert.Nil(t, err)

	err = db.Put([]byte("exist"), []byte("value"), nil)
	db.flushMemtable(db.activeMem)
	assert.Nil(t, err)

	tests := []struct {
		name    string
		key     []byte
		exist   bool
		wantErr bool
	}{
		{"nil", nil, false, true},
		{"not-exist", []byte("not-exist"), false, true},
		{"exist", []byte("exist"), true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := db.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("HashTable.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, value != nil, tt.exist)
			if tt.exist {
				assert.Equal(t, value, []byte("value"))
			}
		})
	}
}

func TestHashTable_DeleteBatch(t *testing.T) {
	testHashTableDeleteBatch(t, 1)
	testHashTableDeleteBatch(t, 3)
}

func testHashTableDeleteBatch(t *testing.T, partitionNum int) {
	options := Options{
		IndexType:       Hash,
		DirPath:         filepath.Join(os.TempDir(), "hashtable-deleteBatch-"+strconv.Itoa(partitionNum)),
		PartitionNum:    partitionNum,
		KeyHashFunction: xxhash.Sum64,
	}
	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()
	db, err := Open(options)
	assert.Nil(t, err)
	ht := db.index.(*HashTable)

	var keys [][]byte
	keys = append(keys, nil, []byte("not-exist"), []byte("exist"))

	err = db.Put([]byte("exist"), []byte("value"), &WriteOptions{})
	assert.Nil(t, err)
	db.flushMemtable(db.activeMem)
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
			matchKeys := make([]diskhash.MatchKeyFunc, len(tt.keys))
			var value []byte
			for i := range matchKeys {
				matchKeys[i] = MatchKeyFunc(db, tt.keys[i], nil, &value)
			}
			if err := ht.DeleteBatch(tt.keys, matchKeys...); (err != nil) != tt.wantErr {
				t.Errorf("HashTable.DeleteBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashTable_Close(t *testing.T) {
	testHashTableClose(t, 1)
	testHashTableClose(t, 3)
}

func testHashTableClose(t *testing.T, partitionNum int) {
	options := Options{
		IndexType:       Hash,
		DirPath:         filepath.Join(os.TempDir(), "hashtable-close-"+strconv.Itoa(partitionNum)),
		PartitionNum:    partitionNum,
		KeyHashFunction: xxhash.Sum64,
	}

	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := Open(options)
	assert.Nil(t, err)
	ht := db.index.(*HashTable)

	err = ht.Close()
	assert.Nil(t, err)
}

func TestHashTable_Sync(t *testing.T) {
	testHashTableSync(t, 1)
	testHashTableSync(t, 3)
}

func testHashTableSync(t *testing.T, partitionNum int) {
	options := Options{
		IndexType:       Hash,
		DirPath:         filepath.Join(os.TempDir(), "hashtable-sync-"+strconv.Itoa(partitionNum)),
		PartitionNum:    partitionNum,
		KeyHashFunction: xxhash.Sum64,
	}

	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := Open(options)
	assert.Nil(t, err)
	ht := db.index.(*HashTable)

	err = ht.Sync()
	assert.Nil(t, err)
}
