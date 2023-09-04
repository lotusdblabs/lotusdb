package lotusdb

import (
	"os"
	"testing"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
)

func TestOpenValueLog(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-open")
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
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
		err = vlog.close()
		assert.Nil(t, err)
	})
}

func TestValueLogWriteAllKindsEntries(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-write-entries")
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
		_ = os.RemoveAll(path)
	}()

	logs := []ValueLogRecord{
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
			"normal key-value", args{log: &logs[0]}, false,
		},
		{
			"nil key", args{log: &logs[1]}, false,
		},
		{
			"nil value", args{log: &logs[2]}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := []ValueLogRecord{*tt.args.log}
			_, err := vlog.writeBatch(logs)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}

	err = vlog.close()
	assert.Nil(t, err)
}

func TestValueLogWriteBatch(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-write-batch")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	numRWList := []int{50000, 100000, 200000}

	tests := []struct {
		name    string
		numPart int
		wantErr bool
	}{
		{"1-part", 1, false},
		{"3-part", 3, false},
		{"10-part", 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, numRW := range numRWList {
				err := writeBatch(opts, numRW, tt.numPart)
				if (err != nil) != tt.wantErr {
					t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
				}
			}
		})
	}

}

func TestValueLogWriteBatchReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-write-batch-reopen")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
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
	err = writeBatch(opts, numRW, numPart)
	assert.Nil(t, err)

	t.Run("writeBatch after reopening", func(t *testing.T) {
		err = writeBatch(opts, numRW, numPart)
		assert.Nil(t, err)
	})
}

// todo change function name
func writeBatch(opts valueLogOptions, numRW int, numPart int) error {
	opts.partitionNum = uint32(numPart)
	vlog, err := openValueLog(opts)
	if err != nil {
		return err
	}

	val := util.RandomValue(512)
	var logs []ValueLogRecord
	for i := 0; i < numRW; i++ {
		log := ValueLogRecord{key: util.GetTestKey(i), value: val}
		logs = append(logs, log)
	}

	_, err = vlog.writeBatch(logs)
	if err != nil {
		return err
	}
	return vlog.close()
}

func TestValueLogRead(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-read")
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
		_ = os.RemoveAll(path)
	}()

	logs := []ValueLogRecord{
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
			"normal key-value", args{log: &logs[0]}, false,
		},
		{
			"nil key", args{log: &logs[1]}, false,
		},
		{
			"nil value", args{log: &logs[2]}, false,
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

	err = vlog.close()
	assert.Nil(t, err)
}

func TestValueLogReadReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-read-reopen")
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
		_ = os.RemoveAll(path)
	}()

	logs := []ValueLogRecord{
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
	err = vlog.close()
	assert.Nil(t, err)

	t.Run("read after reopening vlog", func(t *testing.T) {
		vlog, err = openValueLog(opts)
		assert.Nil(t, err)
		for i := 0; i < len(logs); i++ {
			record, err := vlog.read(pos[i])
			assert.Nil(t, err)
			assert.Equal(t, kv[string(pos[i].key)], string(record.value))
		}
		err = vlog.close()
		assert.Nil(t, err)
	})
}

func TestValueLogSync(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-sync")
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	defer func() {
		_ = os.RemoveAll(path)
	}()

	vlog, err := openValueLog(opts)
	assert.Nil(t, err)

	_, err = vlog.writeBatch([]ValueLogRecord{{key: []byte("key"), value: []byte("value")}})
	assert.Nil(t, err)

	t.Run("test value log sync", func(t *testing.T) {
		err := vlog.sync()
		assert.Nil(t, err)
	})
	err = vlog.close()
	assert.Nil(t, err)
}

func TestValueLogClose(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-close")
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
		_ = os.RemoveAll(path)
	}()

	t.Run("test close value log", func(t *testing.T) {
		err := vlog.close()
		assert.Nil(t, err)
	})
}

func TestValueLogMultiSegmentFiles(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-multi-segment")
	assert.Nil(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     100 * MB,
		blockCache:      DefaultOptions.BlockCache,
		partitionNum:    1,
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	tests := []struct {
		name       string
		wantNumSeg float64
	}{
		{"1 segment files", 1},
		{"3 segment files", 3},
		{"5 segment files", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				_ = os.RemoveAll(path)
			}()

			vlog, err := openValueLog(opts)
			assert.Nil(t, err)

			var logs []ValueLogRecord
			numLogs := (tt.wantNumSeg - 0.5) * 100
			for i := 0; i < int(numLogs); i++ {
				// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
				log := ValueLogRecord{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
				logs = append(logs, log)
			}

			_, err = vlog.writeBatch(logs)
			assert.Nil(t, err)

			entries, err := os.ReadDir(path)
			assert.Nil(t, err)
			assert.Equal(t, int(tt.wantNumSeg), len(entries))

			err = vlog.close()
			assert.Nil(t, err)
		})
	}
}
