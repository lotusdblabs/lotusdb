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
	"github.com/stretchr/testify/require"
)

func testMatchFunc(exist bool) diskhash.MatchKeyFunc {
	return func(_ diskhash.Slot) (bool, error) {
		return exist, nil
	}
}

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
					keyHashFunction: xxhash.Sum64,
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
					keyHashFunction: xxhash.Sum64,
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
			require.NoError(t, err)
			got, ok := db.index.(*HashTable)
			if !ok {
				t.Errorf("indexType wrong")
			}
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
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-putBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}
	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	require.NoError(t, err)

	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       nil,
		partition: 0,
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte("normal"),
		partition: uint32(ht.options.getKeyPartition([]byte("normal"))),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte(""),
		partition: uint32(ht.options.getKeyPartition([]byte(""))),
		position:  &wal.ChunkPosition{},
	})

	matchKeyFuncs := []diskhash.MatchKeyFunc{
		testMatchFunc(false), testMatchFunc(true), testMatchFunc(false),
	}

	tests := []struct {
		name         string
		positions    []*KeyPosition
		matchKeyFunc []diskhash.MatchKeyFunc
		wantErr      bool
	}{
		{"empty", keyPositions[:0], matchKeyFuncs[:0], false},
		{"nil-key", keyPositions[:1], matchKeyFuncs[:1], true},
		{"normal", keyPositions[1:2], matchKeyFuncs[1:2], false},
		{"len(key)=0", keyPositions[2:3], matchKeyFuncs[2:3], true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err = ht.PutBatch(tt.positions, tt.matchKeyFunc...); (err != nil) != tt.wantErr {
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
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-get-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}
	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	ht, err := openHashIndex(options)
	require.NoError(t, err)
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(ht.options.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})
	matchKeyFuncs := []diskhash.MatchKeyFunc{
		testMatchFunc(true), testMatchFunc(false),
	}
	_, err = ht.PutBatch(keyPositions, matchKeyFuncs[:1]...)
	require.NoError(t, err)

	tests := []struct {
		name         string
		key          []byte
		matchKeyFunc []diskhash.MatchKeyFunc
		exist        bool
		wantErr      bool
	}{
		{"nil", nil, matchKeyFuncs[1:], false, true},
		{"not-exist", []byte("not-exist"), matchKeyFuncs[1:], false, false},
		{"exist", []byte("exist"), matchKeyFuncs[:1], true, false},
		{"len(key)=0", []byte(""), matchKeyFuncs[1:], false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = ht.Get(tt.key, tt.matchKeyFunc...)
			if (err != nil) != tt.wantErr {
				t.Errorf("HashTable.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestHashTable_DeleteBatch(t *testing.T) {
	testHashTableDeleteBatch(t, 1)
	testHashTableDeleteBatch(t, 3)
}

func testHashTableDeleteBatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-deleteBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}
	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	ht, err := openHashIndex(options)
	require.NoError(t, err)

	var keys [][]byte
	keys = append(keys, nil, []byte("not-exist"), []byte("exist"), []byte(""))
	var keyPositions []*KeyPosition

	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("normal"),
		partition: uint32(ht.options.getKeyPartition([]byte("normal"))),
		position:  &wal.ChunkPosition{},
	})
	_, err = ht.PutBatch(keyPositions, []diskhash.MatchKeyFunc{testMatchFunc(true)}...)
	require.NoError(t, err)

	matchKeyFuncs := []diskhash.MatchKeyFunc{
		testMatchFunc(false), testMatchFunc(true),
	}
	tests := []struct {
		name         string
		keys         [][]byte
		matchKeyFunc []diskhash.MatchKeyFunc
		wantErr      bool
	}{
		{"nil", keys[:1], matchKeyFuncs[:1], true},
		{"no-exist", keys[1:2], matchKeyFuncs[:1], false},
		{"exist", keys[2:3], matchKeyFuncs[1:], false},
		{"len(key)=0", keys[3:4], matchKeyFuncs[:1], true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err = ht.DeleteBatch(tt.keys, tt.matchKeyFunc...); (err != nil) != tt.wantErr {
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
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-close-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}
	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	require.NoError(t, err)

	err = ht.Close()
	assert.NoError(t, err)
}

func TestHashTable_Sync(t *testing.T) {
	testHashTableSync(t, 1)
	testHashTableSync(t, 3)
}

func testHashTableSync(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       Hash,
		dirPath:         filepath.Join(os.TempDir(), "hashtable-sync-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}
	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	ht, err := openHashIndex(options)
	require.NoError(t, err)

	err = ht.Sync()
	assert.NoError(t, err)
}
