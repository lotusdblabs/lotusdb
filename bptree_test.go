package lotusdb

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func Test_openIndexBoltDB(t *testing.T) {
	tests := []struct {
		name    string
		options indexOptions
		want    *BPTree
		wantErr bool
	}{
		{"normal_1",
			indexOptions{
				indexType:       BTree,
				dirPath:         filepath.Join(os.TempDir(), "bptree-open-1"),
				partitionNum:    1,
				keyHashFunction: xxhash.Sum64,
			},
			&BPTree{
				options: indexOptions{
					indexType:       BTree,
					dirPath:         filepath.Join(os.TempDir(), "bptree-open-1"),
					partitionNum:    1,
					keyHashFunction: xxhash.Sum64,
				},
				trees: make([]*bbolt.DB, 1),
			},
			false,
		},
		{"normal_3",
			indexOptions{
				indexType:       BTree,
				dirPath:         filepath.Join(os.TempDir(), "bptree-open-3"),
				partitionNum:    3,
				keyHashFunction: xxhash.Sum64,
			},
			&BPTree{
				options: indexOptions{
					indexType:       BTree,
					dirPath:         filepath.Join(os.TempDir(), "bptree-open-3"),
					partitionNum:    3,
					keyHashFunction: xxhash.Sum64,
				},
				trees: make([]*bbolt.DB, 3),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.MkdirAll(tt.options.dirPath, os.ModePerm)
			require.NoError(t, err)
			defer func() {
				_ = os.RemoveAll(tt.options.dirPath)
			}()
			got, err := openBTreeIndex(tt.options)
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

func TestBPTree_Get_1(t *testing.T) {
	testbptreeGet(t, 1)
}

func TestBPTree_Get_3(t *testing.T) {
	testbptreeGet(t, 3)
}

func testbptreeGet(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-get-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openBTreeIndex(options)
	require.NoError(t, err)
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(bt.options.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})
	_, err = bt.PutBatch(keyPositions)
	require.NoError(t, err)

	tests := []struct {
		name    string
		key     []byte
		exist   bool
		wantErr bool
	}{
		{"nil", nil, false, true},
		{"not-exist", []byte("not-exist"), false, false},
		{"exist", []byte("exist"), true, false},
		{"len(key)=0", []byte(""), false, true},
	}
	var got *KeyPosition
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err = bt.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("BPTree.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, got != nil, tt.exist)
		})
	}
}

func TestBPTree_PutBatch_1(t *testing.T) {
	testbptreePutbatch(t, 1)
}

func TestBPTree_PutBatch_3(t *testing.T) {
	testbptreePutbatch(t, 3)
}

func testbptreePutbatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-putBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openBTreeIndex(options)
	require.NoError(t, err)

	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       nil,
		partition: 0,
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte("normal"),
		partition: uint32(bt.options.getKeyPartition([]byte("normal"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte(""),
		partition: uint32(bt.options.getKeyPartition([]byte(""))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	},
	)

	tests := []struct {
		name      string
		positions []*KeyPosition
		wantErr   bool
	}{
		{"empty", keyPositions[:0], false},
		{"nil-key", keyPositions[:1], true},
		{"normal", keyPositions[1:2], false},
		{"len(key)=0", keyPositions[2:3], true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err = bt.PutBatch(tt.positions); (err != nil) != tt.wantErr {
				t.Errorf("BPTree.PutBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBPtreePutbatchOldUUID(t *testing.T) {
	partitionNum := 3
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-putBatch-olduuid"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openBTreeIndex(options)
	require.NoError(t, err)

	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("123"),
		partition: uint32(bt.options.getKeyPartition([]byte("123"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte("456"),
		partition: uint32(bt.options.getKeyPartition([]byte("456"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte("789"),
		partition: uint32(bt.options.getKeyPartition([]byte("789"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	},
	)

	var coverKeyPositions []*KeyPosition
	coverKeyPositions = append(coverKeyPositions, &KeyPosition{
		key:       []byte("123"),
		partition: uint32(bt.options.getKeyPartition([]byte("123"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte("456"),
		partition: uint32(bt.options.getKeyPartition([]byte("456"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	}, &KeyPosition{
		key:       []byte("789"),
		partition: uint32(bt.options.getKeyPartition([]byte("789"))),
		uid:       uuid.New(),
		position:  &wal.ChunkPosition{},
	},
	)

	t.Run("check old uuid", func(t *testing.T) {
		_, err = bt.PutBatch(keyPositions)
		if err != nil {
			t.Errorf("put error = %v", err)
		}
		var oldKeyPostions []*KeyPosition
		oldKeyPostions, err = bt.PutBatch(coverKeyPositions)
		if err != nil {
			t.Errorf("put error = %v", err)
		}
		uidMap := make(map[uuid.UUID]struct{})
		for _, oldKeyPostion := range oldKeyPostions {
			uidMap[oldKeyPostion.uid] = struct{}{}
		}
		for _, position := range keyPositions {
			log.Println("keyPositions", position.uid)
		}
		for _, position := range coverKeyPositions {
			log.Println("coverkeyPositions", position.uid)
		}

		for _, position := range keyPositions {
			if _, exists := uidMap[position.uid]; !exists {
				log.Println("now:", position.uid)
				t.Errorf("uuid not exist!")
			}
		}
	})
}

func TestBPTree_DeleteBatch_1(t *testing.T) {
	testbptreeDeletebatch(t, 1)
}

func TestBPTree_DeleteBatch_3(t *testing.T) {
	testbptreeDeletebatch(t, 3)
}

func testbptreeDeletebatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-deleteBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openBTreeIndex(options)
	require.NoError(t, err)
	var keys [][]byte
	keys = append(keys, nil, []byte("not-exist"), []byte("exist"), []byte(""))
	var keyPositions []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("exist"),
		partition: uint32(bt.options.getKeyPartition([]byte("exist"))),
		position:  &wal.ChunkPosition{},
	})

	_, err = bt.PutBatch(keyPositions)
	require.NoError(t, err)

	tests := []struct {
		name    string
		keys    [][]byte
		wantErr bool
	}{
		{"nil", keys[:1], true},
		{"no-exist", keys[1:2], false},
		{"exist", keys[2:3], false},
		{"len(key)=0", keys[3:], true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err = bt.DeleteBatch(tt.keys); (err != nil) != tt.wantErr {
				t.Errorf("BPTree.DeleteBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBPTree_Close_1(t *testing.T) {
	testbptreeClose(t, 1)
}

func TestBPTree_Close_3(t *testing.T) {
	testbptreeClose(t, 3)
}

func testbptreeClose(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-close-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openBTreeIndex(options)
	require.NoError(t, err)

	err = bt.Close()
	assert.NoError(t, err)
}

func TestBPTree_Sync_1(t *testing.T) {
	testbptreeSync(t, 1)
}

func TestBPTree_Sync_3(t *testing.T) {
	testbptreeSync(t, 3)
}

func testbptreeSync(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-sync-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openBTreeIndex(options)
	require.NoError(t, err)
	assert.NotNil(t, bt)
	err = bt.Sync()
	assert.NoError(t, err)
}

func Test_bptreeIterator(t *testing.T) {
	options := indexOptions{
		indexType:       BTree,
		dirPath:         filepath.Join(os.TempDir(), "bptree-cursorIterator"+strconv.Itoa(1)),
		partitionNum:    1,
		keyHashFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openBTreeIndex(options)
	require.NoError(t, err)
	m := map[string]*wal.ChunkPosition{
		"key 0": {SegmentId: 0, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 0},
		"key 1": {SegmentId: 1, BlockNumber: 1, ChunkOffset: 1, ChunkSize: 1},
		"key 2": {SegmentId: 2, BlockNumber: 2, ChunkOffset: 2, ChunkSize: 2},
	}
	m2 := map[string]*wal.ChunkPosition{
		"abc 0": {SegmentId: 3, BlockNumber: 3, ChunkOffset: 3, ChunkSize: 3},
		"abc 1": {SegmentId: 4, BlockNumber: 4, ChunkOffset: 4, ChunkSize: 4},
	}
	var keyPositions, keyPositions2 []*KeyPosition
	keyPositions = append(keyPositions, &KeyPosition{
		key:       []byte("key 0"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 0},
	}, &KeyPosition{
		key:       []byte("key 1"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 1, BlockNumber: 1, ChunkOffset: 1, ChunkSize: 1},
	}, &KeyPosition{
		key:       []byte("key 2"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 2, BlockNumber: 2, ChunkOffset: 2, ChunkSize: 2},
	},
	)

	keyPositions2 = append(keyPositions2, &KeyPosition{
		key:       []byte("key 0"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 0},
	}, &KeyPosition{
		key:       []byte("key 1"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 1, BlockNumber: 1, ChunkOffset: 1, ChunkSize: 1},
	}, &KeyPosition{
		key:       []byte("key 2"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 2, BlockNumber: 2, ChunkOffset: 2, ChunkSize: 2},
	}, &KeyPosition{
		key:       []byte("abc 0"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 3, BlockNumber: 3, ChunkOffset: 3, ChunkSize: 3},
	}, &KeyPosition{
		key:       []byte("key abc"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 4, BlockNumber: 4, ChunkOffset: 4, ChunkSize: 4},
	}, &KeyPosition{
		key:       []byte("abc 1"),
		partition: 0,
		position:  &wal.ChunkPosition{SegmentId: 4, BlockNumber: 4, ChunkOffset: 4, ChunkSize: 4},
	})

	_, err = bt.PutBatch(keyPositions)
	require.NoError(t, err)

	tree := bt.trees[0]
	tx, err := tree.Begin(true)
	require.NoError(t, err)
	iteratorOptions := IteratorOptions{
		Reverse: false,
	}

	itr := newBptreeIterator(tx, iteratorOptions)
	require.NoError(t, err)
	var prev []byte
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == -1)
		assert.Equal(t, m[string(itr.Key())].Encode(), itr.Value())
		prev = currKey
		itr.Next()
	}
	err = itr.Close()
	require.NoError(t, err)

	tx, err = tree.Begin(true)
	require.NoError(t, err)
	iteratorOptions = IteratorOptions{
		Reverse: true,
	}
	prev = nil

	itr = newBptreeIterator(tx, iteratorOptions)
	require.NoError(t, err)
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == 1)
		assert.Equal(t, m[string(itr.Key())].Encode(), itr.Value())
		prev = currKey
		itr.Next()
	}
	itr.Seek([]byte("key 4"))
	assert.Equal(t, []byte("key 2"), itr.Key())

	itr.Seek([]byte("key 2"))
	assert.Equal(t, []byte("key 2"), itr.Key())

	itr.Seek([]byte("aye 2"))
	assert.False(t, itr.Valid())
	err = itr.Close()
	require.NoError(t, err)

	tx, err = tree.Begin(true)
	require.NoError(t, err)
	iteratorOptions = IteratorOptions{
		Reverse: false,
	}
	prev = nil

	itr = newBptreeIterator(tx, iteratorOptions)
	require.NoError(t, err)
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == -1)
		assert.Equal(t, m[string(itr.Key())].Encode(), itr.Value())
		prev = currKey
		itr.Next()
	}

	itr.Seek([]byte("key 0"))
	assert.Equal(t, []byte("key 0"), itr.Key())
	itr.Seek([]byte("key 4"))
	assert.False(t, itr.Valid())

	itr.Seek([]byte("aye 2"))
	assert.Equal(t, []byte("key 0"), itr.Key())
	err = itr.Close()
	require.NoError(t, err)

	// prefix
	_, err = bt.PutBatch(keyPositions2)
	require.NoError(t, err)

	tx, err = tree.Begin(true)
	require.NoError(t, err)
	iteratorOptions = IteratorOptions{
		Reverse: false,
		Prefix:  []byte("not valid"),
	}

	itr = newBptreeIterator(tx, iteratorOptions)
	require.NoError(t, err)
	itr.Rewind()
	assert.False(t, itr.Valid())
	err = itr.Close()
	require.NoError(t, err)

	tx, err = tree.Begin(true)
	require.NoError(t, err)
	iteratorOptions = IteratorOptions{
		Reverse: false,
		Prefix:  []byte("abc"),
	}

	itr = newBptreeIterator(tx, iteratorOptions)
	require.NoError(t, err)
	itr.Rewind()
	assert.True(t, itr.Valid())

	for itr.Valid() {
		assert.True(t, bytes.HasPrefix(itr.Key(), iteratorOptions.Prefix))
		assert.Equal(t, m2[string(itr.Key())].Encode(), itr.Value())
		itr.Next()
	}
	err = itr.Close()
	assert.NoError(t, err)
}
