package lotusdb

// ----------------------------------------------
// | case 1 | TestOpenValueLog				    |
// ----------------------------------------------
// | case 2 | TestValueLogWriteAllKindsEntries  |
// ----------------------------------------------
// | case 3 | TestValueLogWriteBatch			|
// ----------------------------------------------
// | case 4 | TestValueLogWriteBatchReopen		|
// ----------------------------------------------
// | case 5 | TestValueLogRead					|
// ----------------------------------------------
// | case 6 | TestValueLogReadReopen			|
// ----------------------------------------------
// | case 7 | TestValueLogSync					|
// ---------------------------------------------=
// | case 8 | TestValueLogClose					|
// ---------------------------------------------=
// | case 9 | TestValueLogCompaction			|
// ---------------------------------------------=

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpenValueLog(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}
	t.Run("open vlog files", func(t *testing.T) {
		vlog, err := openValueLog(opts)
		assert.Nil(t, err)
		vlog.close()
	})

}

func TestValueLogWriteAllKindsEntries(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	logs := []*ValueLogRecord{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: nil, value: []byte("value 2")},
		{key: []byte("key 3"), value: nil},
	}

	type args struct {
		log *ValueLogRecord
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"normal key-value", args{log: logs[0]}, false,
		},
		{
			"nil key", args{log: logs[1]}, false,
		},
		{
			"nil value", args{log: logs[2]}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := []*ValueLogRecord{tt.args.log}

			_, err := vlog.writeBatch(logs)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}

	t.Run("write all kinds of entries", func(t *testing.T) {
		_, err := vlog.writeBatch(logs)
		assert.Nil(t, err)
	})

	vlog.close()
}

// === RUN   TestValueLogWriteBatch
// numRW:500000
// PartitionNum:1 write:8.4933054s
// PartitionNum:3 write:4.6630058s
// PartitionNum:10 write:3.790305s
// numRW:1000000
// PartitionNum:1 write:18.2333748s
// PartitionNum:3 write:8.5497644s
// PartitionNum:10 write:8.0717735s
// numRW:2000000
// PartitionNum:1 write:34.7807686s
// PartitionNum:3 write:17.3591007s
// PartitionNum:10 write:15.194792s
// --- PASS: TestValueLogWriteBatch (125.65s)
// PASS
// ok      github.com/lotusdblabs/lotusdb/v2       126.758s
// Please set the go-test timeout threshold high enough before runing this test, about 360s.
func TestValueLogWriteBatch(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	numRWList := []int{500000, 1000000, 2000000}
	numPartList := []int{1, 3, 10}
	t.Run("writeBatch normally", func(t *testing.T) {
		for _, numRW := range numRWList {
			fmt.Printf("numRW:%d\n", numRW)
			for _, numPart := range numPartList {
				fmt.Printf("numPart:%d ", numPart)
				err := RWBatch(opts, numRW, numPart)
				assert.Nil(t, err)
			}
		}
	})

}

func TestValueLogWriteBatchReopen(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	numRW := 50000
	numPart := 10
	err = RWBatch(opts, numRW, numPart)
	assert.Nil(t, err)

	t.Run("writeBatch after reopen", func(t *testing.T) {
		err = RWBatch(opts, numRW, numPart)
		assert.Nil(t, err)
	})
}

func RWBatch(opts valueLogOptions, numRW int, numPart int) error {
	opts.partitionNum = uint32(numPart)
	vlog, err := openValueLog(opts)
	if err != nil {
		return err
	}

	val := strings.Repeat("v", 512)
	logs := []*ValueLogRecord{}
	for i := 0; i < numRW; i++ {
		log := &ValueLogRecord{key: []byte(fmt.Sprintf("%d", i)), value: []byte(val)}
		logs = append(logs, log)
	}
	start := time.Now()
	_, err = vlog.writeBatch(logs)
	if err != nil {
		return err
	}
	end := time.Since(start)
	fmt.Printf("write:%v\n", end)

	vlog.close()
	return nil
}

func TestValueLogRead(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	logs := []*ValueLogRecord{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: nil, value: []byte("value 2")},
		{key: []byte("key 3"), value: nil},
	}
	kv := map[string]string{
		"key 0": "value 0",
		"":      "value 2",
		"key 3": "",
	}

	type args struct {
		log *ValueLogRecord
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"normal key-value", args{log: logs[0]}, false,
		},
		{
			"nil key", args{log: logs[1]}, false,
		},
		{
			"nil value", args{log: logs[2]}, false,
		},
	}

	pos, err := vlog.writeBatch(logs[:])
	assert.Nil(t, err)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readLogs, err := vlog.read(pos[i])
			if (err != nil) != tt.wantErr {
				t.Errorf("read(pos) error = %v, wantErr = %v", err, tt.wantErr)
			}
			assert.Equal(t, kv[string(pos[i].key)], string(readLogs.value))
		})
	}

	t.Run("read after reopening vlog", func(t *testing.T) {
		vlog.close()
		vlog, err = openValueLog(opts)
		assert.Nil(t, err)
		for i := 0; i < len(logs); i++ {
			readLogs, err := vlog.read(pos[i])
			assert.Nil(t, err)
			assert.Equal(t, kv[string(pos[i].key)], string(readLogs.value))
		}
	})

	vlog.close()
}

func TestValueLogReadReopen(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	logs := []*ValueLogRecord{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: nil, value: []byte("value 2")},
		{key: []byte("key 3"), value: nil},
	}
	kv := map[string]string{
		"key 0": "value 0",
		"":      "value 2",
		"key 3": "",
	}

	pos, err := vlog.writeBatch(logs[:])
	assert.Nil(t, err)

	t.Run("read after reopening vlog", func(t *testing.T) {
		vlog.close()
		vlog, err = openValueLog(opts)
		assert.Nil(t, err)
		for i := 0; i < len(logs); i++ {
			readLogs, err := vlog.read(pos[i])
			assert.Nil(t, err)
			assert.Equal(t, kv[string(pos[i].key)], string(readLogs.value))
		}
	})

	vlog.close()
}

func TestValueLogSync(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	defer func() {
		os.RemoveAll(path)
	}()

	vlog, err := openValueLog(opts)
	assert.Nil(t, err)

	_, err = vlog.writeBatch([]*ValueLogRecord{{key: []byte("key"), value: []byte("value")}})
	assert.Nil(t, err)

	t.Run("test value log sync", func(t *testing.T) {
		err := vlog.sync()
		assert.Nil(t, err)
	})
	vlog.close()

}

func TestValueLogClose(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	assert.Nil(t, err)

	defer func() {
		os.RemoveAll(path)
	}()

	t.Run("test close value log", func(t *testing.T) {
		err := vlog.close()
		assert.Nil(t, err)
	})
}

func TestValueLogCompaction(t *testing.T) {

}
