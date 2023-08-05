package lotusdb

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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
		{"normal_1",
			indexOptions{
				indexType:       indexBoltDB,
				dirPath:         filepath.Join(os.TempDir(), "bptree-open-1"),
				partitionNum:    1,
				hashKeyFunction: xxhash.Sum64,
			},
			&BPTree{
				options: indexOptions{
					indexType:       indexBoltDB,
					dirPath:         filepath.Join(os.TempDir(), "bptree-open-1"),
					partitionNum:    1,
					hashKeyFunction: xxhash.Sum64,
				},
				trees: make([]*bbolt.DB, 1),
			},
			false,
		},
		{"normal_3",
			indexOptions{
				indexType:       indexBoltDB,
				dirPath:         filepath.Join(os.TempDir(), "bptree-open-3"),
				partitionNum:    3,
				hashKeyFunction: xxhash.Sum64,
			},
			&BPTree{
				options: indexOptions{
					indexType:       indexBoltDB,
					dirPath:         filepath.Join(os.TempDir(), "bptree-open-3"),
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
			err := os.MkdirAll(tt.options.dirPath, os.ModePerm)
			assert.Nil(t, err)
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
	testbptreeGet(t, 1)
	testbptreeGet(t, 3)
}

func testbptreeGet(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-get-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
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
			assert.Equal(t, got != nil, tt.exist)
		})
	}
}

func TestBPTree_PutBatch(t *testing.T) {
	testbptreePutbatch(t, 1)
	testbptreePutbatch(t, 3)

}

func testbptreePutbatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-putBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
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
	testbptreeDeletebatch(t, 1)
	testbptreeDeletebatch(t, 3)
}

func testbptreeDeletebatch(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-deleteBatch-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
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
	testbptreeClose(t, 1)
	testbptreeClose(t, 3)
}

func testbptreeClose(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-close-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)

	err = bt.Close()
	assert.Nil(t, err)
}

func TestBPTree_getKeyPartition(t *testing.T) {
	testbptreeGetkeypartition(t, 1)
	testbptreeGetkeypartition(t, 3)
}

func testbptreeGetkeypartition(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-getKeyPartition-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
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
	testbptreeSync(t, 1)
	testbptreeSync(t, 3)
}

func testbptreeSync(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-sync-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	err = bt.Sync()
	assert.Nil(t, err)
}

func TestBPTree_Iterator(t *testing.T) {
	testBPTree_Iterator(t, 1)
	testBPTree_Iterator(t, 3)
}

func testBPTree_Iterator(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-iterator-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)

	tests := []struct {
		name    string
		reverse bool
	}{
		{"normal", false},
		{"normal", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bt.Iterator(tt.reverse).(*bptreeIterator)
			assert.NotNil(t, got)
			assert.NotNil(t, got.h)
			assert.Equal(t, cap(got.marks), bt.options.partitionNum)
			assert.Equal(t, cap(got.txs), bt.options.partitionNum)
			assert.Equal(t, cap(got.cursors), bt.options.partitionNum)
			assert.Equal(t, got.partitionNum, bt.options.partitionNum)
			assert.Equal(t, got.reverse, tt.reverse)
		})
	}
}

func Test_bptreeIterator_Rewind(t *testing.T) {
	test_bptreeIterator_Rewind(t, 1)
	test_bptreeIterator_Rewind(t, 3)
}

func test_bptreeIterator_Rewind(t *testing.T, partitionNum int) {
	tests := []struct {
		name    string
		reverse bool
		options *indexOptions
		keyNum  int
	}{
		{"empty-descend", true, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-rewind-"+strconv.Itoa(partitionNum)+"-empty-descend"),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 0},
		{"empty-ascend", false, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-rewind-"+strconv.Itoa(partitionNum)+"-empty-ascend"),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 0},
		{"normal-descend", true, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-rewind-"+strconv.Itoa(partitionNum)+"-normal-descend"),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 5},
		{"normal-ascend", false, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-rewind-"+strconv.Itoa(partitionNum)+"-normal-ascend"),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.options.dirPath = filepath.Join(tt.options.dirPath, tt.name)
			err := os.MkdirAll(tt.options.dirPath, os.ModePerm)
			assert.Nil(t, err)
			defer func() {
				_ = os.RemoveAll(tt.options.dirPath)
			}()

			bt, err := openIndexBoltDB(*(tt.options))
			assert.Nil(t, err)
			var keyPositions []*KeyPosition
			var minKey, maxKey []byte
			for i := 0; i < tt.keyNum; i++ {
				key := util.RandomValue(10)
				if minKey == nil || bytes.Compare(key, minKey) == -1 {
					minKey = key
				}
				if maxKey == nil || bytes.Compare(key, maxKey) == 1 {
					maxKey = key
				}
				keyPositions = append(keyPositions, &KeyPosition{
					key:       key,
					partition: uint32(bt.getKeyPartition(key)),
					position:  &wal.ChunkPosition{},
				})
			}
			err = bt.PutBatch(keyPositions)
			assert.Nil(t, err)
			bpi := bt.Iterator(tt.reverse).(*bptreeIterator)
			bpi.Rewind()

			if tt.keyNum == 0 {
				assert.Zero(t, bpi.h.Len())
			} else {
				if tt.reverse {
					assert.Equal(t, maxKey, bpi.h.Top().(*KeyPosition).key)
				} else {
					assert.Equal(t, minKey, bpi.h.Top().(*KeyPosition).key)
				}
			}

			bpi.Close()
		})
	}
}

func Test_bptreeIterator_Seek(t *testing.T) {
	test_bptreeIterator_Seek(t, 1)
	test_bptreeIterator_Seek(t, 3)
}

func test_bptreeIterator_Seek(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-seek-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()

	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keyPositions []*KeyPosition
	minKey := []byte{}
	var maxKey []byte
	for i := 0; i < 20; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(bt.getKeyPartition(key)),
			position:  &wal.ChunkPosition{},
		})
		if maxKey == nil || bytes.Compare(key, key) == 1 {
			maxKey = key
		}
	}
	err = bt.PutBatch(keyPositions[:10])
	assert.Nil(t, err)
	maxKey = append(maxKey, '1')
	tests := []struct {
		name    string
		reverse bool
		key     []byte
	}{
		{"nil", true, minKey},
		{"nil", false, maxKey},
		{"equal", true, keyPositions[0].key},
		{"equal", true, keyPositions[0].key},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpi := bt.Iterator(tt.reverse)
			bpi.Seek(tt.key)
			switch tt.name {
			case "empty":
				assert.False(t, bpi.Valid())
			case "equal":
				assert.True(t, bpi.Valid())
				assert.True(t, reflect.DeepEqual(keyPositions[0], bpi.Value()))
				assert.Equal(t, keyPositions[0].key, bpi.Key())
			}
			bpi.Close()
		})
	}
}

func Test_bptreeIterator_Next(t *testing.T) {
	test_bptreeIterator_Next(t, 1)
	test_bptreeIterator_Next(t, 3)
}

func test_bptreeIterator_Next(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-next-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keyPositions []*KeyPosition
	for i := 0; i < 20; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(bt.getKeyPartition(key)),
			position:  &wal.ChunkPosition{},
		})
	}
	err = bt.PutBatch(keyPositions)
	assert.Nil(t, err)
	tests := []struct {
		name    string
		reverse bool
	}{
		{"ascend", false},
		{"descend", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpi := bt.Iterator(tt.reverse)
			var prev []byte
			for ; bpi.Valid(); bpi.Next() {
				if prev != nil {
					if tt.reverse {
						assert.Less(t, bpi.Key(), prev)
					} else {
						assert.Greater(t, bpi.Key(), prev)
					}
				}
				prev = bpi.Key()
			}
			bpi.Close()
		})
	}
}

func Test_bptreeIterator_Key(t *testing.T) {
	test_bptreeIterator_Key(t, 1)
	test_bptreeIterator_Key(t, 3)
}

func test_bptreeIterator_Key(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-key-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keyPositions []*KeyPosition
	for i := 0; i < 20; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(bt.getKeyPartition(key)),
			position:  &wal.ChunkPosition{},
		})
	}
	err = bt.PutBatch(keyPositions)
	assert.Nil(t, err)

	tests := []struct {
		name    string
		reverse bool
	}{
		{"ascend", false},
		{"descend", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpi := bt.Iterator(tt.reverse)
			assert.Equal(t, bpi.Key(), bpi.Key())
			bpi.Next()
			assert.Equal(t, bpi.Key(), bpi.Key())
			bpi.Next()
			assert.Equal(t, bpi.Key(), bpi.Key())
			bpi.Rewind()
			assert.Equal(t, bpi.Key(), bpi.Key())
			bpi.Close()
		})
	}
}

func Test_bptreeIterator_Value(t *testing.T) {
	test_bptreeIterator_Value(t, 1)
	test_bptreeIterator_Value(t, 3)
}

func test_bptreeIterator_Value(t *testing.T, partitionNum int) {
	options := indexOptions{
		indexType:       indexBoltDB,
		dirPath:         filepath.Join(os.TempDir(), "bptree-value-"+strconv.Itoa(partitionNum)),
		partitionNum:    partitionNum,
		hashKeyFunction: xxhash.Sum64,
	}

	err := os.MkdirAll(options.dirPath, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(options.dirPath)
	}()
	bt, err := openIndexBoltDB(options)
	assert.Nil(t, err)
	var keyPositions []*KeyPosition
	for i := 0; i < 20; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(bt.getKeyPartition(key)),
			position:  &wal.ChunkPosition{},
		})
	}
	err = bt.PutBatch(keyPositions)
	assert.Nil(t, err)

	tests := []struct {
		name    string
		reverse bool
	}{
		{"ascend", false},
		{"descend", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpi := bt.Iterator(tt.reverse)
			assert.True(t, reflect.DeepEqual(bpi.Value(), bpi.Value()))
			bpi.Next()
			assert.True(t, reflect.DeepEqual(bpi.Value(), bpi.Value()))
			bpi.Next()
			assert.True(t, reflect.DeepEqual(bpi.Value(), bpi.Value()))
			bpi.Rewind()
			assert.True(t, reflect.DeepEqual(bpi.Value(), bpi.Value()))
			bpi.Close()
		})
	}
}

func Test_bptreeIterator_Valid(t *testing.T) {
	test_bptreeIterator_Valid(t, 1)
	test_bptreeIterator_Valid(t, 3)
}

func test_bptreeIterator_Valid(t *testing.T, partitionNum int) {
	tests := []struct {
		name    string
		reverse bool
		options *indexOptions
		keyNum  int
		want    bool
	}{
		{"empty", true, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-valid-"+strconv.Itoa(partitionNum)),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 0, false},
		{"empty", false, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-valid-"+strconv.Itoa(partitionNum)),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 0, false},
		{"normal", true, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-valid-"+strconv.Itoa(partitionNum)),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 5, true},
		{"normal", false, &indexOptions{
			indexType:       indexBoltDB,
			dirPath:         filepath.Join(os.TempDir(), "bptree-valid-"+strconv.Itoa(partitionNum)),
			partitionNum:    partitionNum,
			hashKeyFunction: xxhash.Sum64,
		}, 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.options.dirPath = filepath.Join(tt.options.dirPath, tt.name)
			err := os.MkdirAll(tt.options.dirPath, os.ModePerm)
			assert.Nil(t, err)
			defer func() {
				_ = os.RemoveAll(tt.options.dirPath)
			}()
			bt, err := openIndexBoltDB(*(tt.options))
			assert.Nil(t, err)
			var keyPositions []*KeyPosition
			for i := 0; i < tt.keyNum; i++ {
				key := util.RandomValue(10)
				keyPositions = append(keyPositions, &KeyPosition{
					key:       key,
					partition: uint32(bt.getKeyPartition(key)),
					position:  &wal.ChunkPosition{},
				})
			}
			err = bt.PutBatch(keyPositions)
			assert.Nil(t, err)
			bpi := bt.Iterator(tt.reverse).(*bptreeIterator)
			assert.Equal(t, bpi.Valid(), tt.want)
			bpi.Close()
		})
	}
}

func test_keyPositionHeap_Len(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}

	tests := []struct {
		name    string
		values  []*KeyPosition
		reverse bool
		want    int
	}{
		{"empty-ascend", keyPositions[:0], false, 0},
		{"empty-descend", keyPositions[:0], true, 0},
		{"normal-ascend", keyPositions, false, 10},
		{"normal-descend", keyPositions, true, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := keyPositionHeap{
				values:  tt.values,
				reverse: tt.reverse,
			}
			if got := h.Len(); got != tt.want {
				t.Errorf("keyPositionHeap.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keyPositionHeap_Less(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}

	type fields struct {
		values  []*KeyPosition
		reverse bool
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"ascend", fields{keyPositions, false}, args{0, 1}},
		{"ascend", fields{keyPositions, false}, args{0, 9}},
		{"descend", fields{keyPositions, true}, args{0, 1}},
		{"descend", fields{keyPositions, true}, args{0, 9}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := keyPositionHeap{
				values:  tt.fields.values,
				reverse: tt.fields.reverse,
			}
			left, right := h.values[tt.args.i], h.values[tt.args.j]
			if !tt.fields.reverse {
				assert.Equal(t, bytes.Compare(left.key, right.key) == -1, h.Less(tt.args.i, tt.args.j))
			} else {
				assert.Equal(t, bytes.Compare(left.key, right.key) == 1, h.Less(tt.args.i, tt.args.j))
			}

		})
	}
}

func Test_keyPositionHeap_Swap(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}
	type fields struct {
		values  []*KeyPosition
		reverse bool
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"ascend", fields{keyPositions, false}, args{0, 1}},
		{"ascend", fields{keyPositions, false}, args{0, 9}},
		{"descend", fields{keyPositions, true}, args{0, 1}},
		{"descend", fields{keyPositions, true}, args{0, 9}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := keyPositionHeap{
				values:  tt.fields.values,
				reverse: tt.fields.reverse,
			}
			left, right := h.values[tt.args.i], h.values[tt.args.j]
			h.Swap(tt.args.i, tt.args.j)
			assert.True(t, reflect.DeepEqual(left, h.values[tt.args.j]))
			assert.True(t, reflect.DeepEqual(right, h.values[tt.args.i]))
		})
	}
}

func Test_keyPositionHeap_Push(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}
	type fields struct {
		values  []*KeyPosition
		reverse bool
	}
	type args struct {
		x any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantIdx int
	}{
		{"empty", fields{keyPositions[:0], false}, args{&KeyPosition{
			key:       util.RandomValue(10),
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		}}, 0},
		{"empty", fields{keyPositions[:0], true}, args{&KeyPosition{
			key:       util.RandomValue(10),
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		}}, 0},
		{"normal", fields{keyPositions[:5], false}, args{&KeyPosition{
			key:       util.RandomValue(10),
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		}}, 5},
		{"normal", fields{keyPositions[:5], true}, args{&KeyPosition{
			key:       util.RandomValue(10),
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		}}, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &keyPositionHeap{
				values:  tt.fields.values,
				reverse: tt.fields.reverse,
			}
			h.Push(tt.args.x)
			assert.True(t, reflect.DeepEqual(tt.args.x, h.values[tt.wantIdx]))
		})
	}
}

func Test_keyPositionHeap_Pop(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}
	type fields struct {
		values  []*KeyPosition
		reverse bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantLen int
		wantErr error
	}{
		{"normal", fields{keyPositions[:5], false}, 5, nil},
		{"normal", fields{keyPositions[:5], true}, 5, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &keyPositionHeap{
				values:  tt.fields.values,
				reverse: tt.fields.reverse,
			}
			assert.Equal(t, h.Len(), tt.wantLen)
			got := h.Pop().(*KeyPosition)
			assert.True(t, reflect.DeepEqual(keyPositions[tt.wantLen-1], got))
			assert.Equal(t, h.Len(), tt.wantLen-1)
		})
	}
}

func Test_keyPositionHeap_Top(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}

	type fields struct {
		values  []*KeyPosition
		reverse bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantIdx int
	}{
		{"normal", fields{keyPositions[:5], false}, 0},
		{"normal", fields{keyPositions[:5], true}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &keyPositionHeap{
				values:  tt.fields.values,
				reverse: tt.fields.reverse,
			}
			if got := h.Top(); !reflect.DeepEqual(got, h.values[tt.wantIdx]) {
				t.Errorf("keyPositionHeap.Top() = %v, want %v", got, h.values[tt.wantIdx])
			}
		})
	}
}

func Test_keyPositionHeap_Clear(t *testing.T) {
	var keyPositions []*KeyPosition
	for i := 0; i < 10; i++ {
		key := util.RandomValue(10)
		keyPositions = append(keyPositions, &KeyPosition{
			key:       key,
			partition: uint32(0),
			position:  &wal.ChunkPosition{},
		})
	}
	type fields struct {
		values  []*KeyPosition
		reverse bool
	}
	tests := []struct {
		name    string
		fields  fields
		oldLen  int
		wantLen int
	}{
		{"empty", fields{keyPositions[:0], false}, 0, 0},
		{"empty", fields{keyPositions[:0], true}, 0, 0},
		{"normal", fields{keyPositions[:5], false}, 5, 0},
		{"normal", fields{keyPositions[:5], true}, 5, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &keyPositionHeap{
				values:  tt.fields.values,
				reverse: tt.fields.reverse,
			}
			assert.Equal(t, tt.oldLen, h.Len())
			h.Clear()
			assert.Equal(t, tt.wantLen, h.Len())
		})
	}
}

func TestNewKeyPositionHeap(t *testing.T) {
	type args struct {
		reverse      bool
		partitionNum int
	}
	tests := []struct {
		name string
		args args
		want *keyPositionHeap
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewKeyPositionHeap(tt.args.reverse, tt.args.partitionNum); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKeyPositionHeap() = %v, want %v", got, tt.want)
			}
		})
	}
}
