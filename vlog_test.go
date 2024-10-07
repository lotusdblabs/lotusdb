package lotusdb

import (
	"os"
	"testing"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenValueLog(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-open")
	require.NoError(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}
	t.Run("open vlog files", func(t *testing.T) {
		vlog, errOpen := openValueLog(opts)
		require.NoError(t, errOpen)
		err = vlog.close()
		assert.NoError(t, err)
	})
}

func TestValueLogWriteAllKindsEntries(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-write-entries")
	require.NoError(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
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
			logsT := []*ValueLogRecord{tt.args.log}
			_, err = vlog.writeBatch(logsT)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}

	err = vlog.close()
	assert.NoError(t, err)
}

func TestValueLogWriteBatch(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-write-batch")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
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
				err = writeBatch(opts, numRW, tt.numPart)
				if (err != nil) != tt.wantErr {
					t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestValueLogWriteBatchReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-write-batch-reopen")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	numRW := 50000
	numPart := 10
	err = writeBatch(opts, numRW, numPart)
	require.NoError(t, err)

	t.Run("writeBatch after reopening", func(t *testing.T) {
		err = writeBatch(opts, numRW, numPart)
		assert.NoError(t, err)
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
	var logs []*ValueLogRecord
	for i := 0; i < numRW; i++ {
		log := &ValueLogRecord{key: util.GetTestKey(int64(i)), value: val}
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
	require.NoError(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
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

	pos, err := vlog.writeBatch(logs)
	require.NoError(t, err)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readLogs, errRead := vlog.read(pos[i])
			if (errRead != nil) != tt.wantErr {
				t.Errorf("read(pos) error = %v, wantErr = %v", err, tt.wantErr)
			}
			assert.Equal(t, kv[string(pos[i].key)], string(readLogs.value))
		})
	}

	err = vlog.close()
	assert.NoError(t, err)
}

func TestValueLogReadReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-read-reopen")
	require.NoError(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
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

	pos, err := vlog.writeBatch(logs)
	require.NoError(t, err)
	err = vlog.close()
	require.NoError(t, err)

	t.Run("read after reopening vlog", func(t *testing.T) {
		vlog, err = openValueLog(opts)
		require.NoError(t, err)
		for i := 0; i < len(logs); i++ {
			record, errRead := vlog.read(pos[i])
			require.NoError(t, errRead)
			assert.Equal(t, kv[string(pos[i].key)], string(record.value))
		}
		err = vlog.close()
		assert.NoError(t, err)
	})
}

func TestValueLogSync(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-sync")
	require.NoError(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	defer func() {
		_ = os.RemoveAll(path)
	}()

	vlog, err := openValueLog(opts)
	require.NoError(t, err)

	_, err = vlog.writeBatch([]*ValueLogRecord{{key: []byte("key"), value: []byte("value")}})
	require.NoError(t, err)

	t.Run("test value log sync", func(t *testing.T) {
		errSync := vlog.sync()
		assert.NoError(t, errSync)
	})
	err = vlog.close()
	assert.NoError(t, err)
}

func TestValueLogClose(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-close")
	require.NoError(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     GB,
		partitionNum:    uint32(DefaultOptions.PartitionNum),
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(opts)
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	t.Run("test close value log", func(t *testing.T) {
		errClose := vlog.close()
		assert.NoError(t, errClose)
	})
}

func TestValueLogMultiSegmentFiles(t *testing.T) {
	path, err := os.MkdirTemp("", "vlog-test-multi-segment")
	require.NoError(t, err)
	opts := valueLogOptions{
		dirPath:         path,
		segmentSize:     100 * MB,
		partitionNum:    1,
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	tests := []struct {
		name       string
		NumSeg     float64
		wantNumSeg int
		want       error
	}{
		{"1 segment files", 1, 1, nil},
		{"3 segment files", 3, 1, wal.ErrPendingSizeTooLarge},
		{"5 segment files", 5, 1, wal.ErrPendingSizeTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				_ = os.RemoveAll(path)
			}()

			vlog, errOpen := openValueLog(opts)
			require.NoError(t, errOpen)

			var logs []*ValueLogRecord
			numLogs := (tt.NumSeg - 0.5) * 100
			for i := 0; i < int(numLogs); i++ {
				// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
				log := &ValueLogRecord{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
				logs = append(logs, log)
			}

			_, err = vlog.writeBatch(logs)
			assert.Equal(t, tt.want, err)

			entries, errRead := os.ReadDir(path)
			require.NoError(t, errRead)
			assert.Len(t, entries, tt.wantNumSeg)

			err = vlog.close()
			assert.NoError(t, err)
		})
	}
}
