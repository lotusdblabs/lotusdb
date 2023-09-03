package lotusdb

import (
	"os"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
)

func TestMemtableOpen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-open")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}

	t.Run("open memtable", func(t *testing.T) {
		table, err := openMemtable(opts)
		assert.Nil(t, err)
		err = table.close()
		assert.Nil(t, err)
	})
}

func TestMemtableOpenAll(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-open-all")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	for i := 0; i < DefaultOptions.MemtableNums; i++ {
		opts := memtableOptions{
			dirPath:         path,
			tableId:         uint32(i),
			memSize:         DefaultOptions.MemtableSize,
			walBytesPerSync: DefaultOptions.BytesPerSync,
			walSync:         DefaultBatchOptions.Sync,
			walBlockCache:   DefaultOptions.BlockCache,
		}
		table, err := openMemtable(opts)
		assert.Nil(t, err)
		err = table.close()
		assert.Nil(t, err)
	}

	t.Run("test open all memtables", func(t *testing.T) {
		var opts = DefaultOptions
		opts.DirPath = path
		tables, err := openAllMemtables(opts)
		assert.Nil(t, err)
		for _, table := range tables {
			err = table.close()
			assert.Nil(t, err)
		}
	})

}

func TestMemTablePutAllKindsEntries(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-all-kinds-entries")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	table, err := openMemtable(opts)
	assert.Nil(t, err)

	writeOpts := &WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	assert.Nil(t, err)

	logs := []*LogRecord{
		{Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		{Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
		{Key: []byte("key 2"), Value: nil, Type: LogRecordNormal},
		{Key: []byte("key 0"), Value: nil, Type: LogRecordDeleted},
	}

	type args struct {
		entry map[string]*LogRecord
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"normal key-value", args{map[string]*LogRecord{string(logs[0].Key): logs[0]}}, false},
		{"nil key", args{map[string]*LogRecord{string(logs[1].Key): logs[1]}}, false},
		{"nil value", args{map[string]*LogRecord{string(logs[2].Key): logs[2]}}, false},
		{"delete key-value", args{map[string]*LogRecord{string(logs[3].Key): logs[3]}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := table.putBatch(tt.args.entry, node.Generate(), writeOpts)
			assert.Nil(t, err)
		})
	}

	err = table.close()
	assert.Nil(t, err)
}

func TestMemTablePutBatch(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-batch")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	table, err := openMemtable(opts)
	assert.Nil(t, err)

	writeOpts := &WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	assert.Nil(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(i), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	t.Run("test memory table put batch", func(t *testing.T) {
		err := table.putBatch(pendingWrites, node.Generate(), writeOpts)
		assert.Nil(t, err)
	})

	err = table.close()
	assert.Nil(t, err)
}

func TestMemTablePutBatchReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-batch-reopen")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	table, err := openMemtable(opts)
	assert.Nil(t, err)

	writeOpts := &WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	assert.Nil(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(i), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
	assert.Nil(t, err)

	t.Run("test memory table put batch after reopening", func(t *testing.T) {
		err = table.close()
		assert.Nil(t, err)
		table, err := openMemtable(opts)
		assert.Nil(t, err)
		err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
		assert.Nil(t, err)
	})

	err = table.close()
	assert.Nil(t, err)

}

func TestMemTableGet(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-get")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	table, err := openMemtable(opts)
	assert.Nil(t, err)

	writeOpts := &WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	assert.Nil(t, err)

	writeLogs := map[string]*LogRecord{
		"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		"":      {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
		"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
	}
	deleteLogs := map[string]*LogRecord{
		"key 0": {Key: []byte("key 0"), Value: []byte(""), Type: LogRecordDeleted},
		"":      {Key: nil, Value: []byte(""), Type: LogRecordDeleted},
		"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordDeleted},
	}

	err = table.putBatch(writeLogs, node.Generate(), writeOpts)
	assert.Nil(t, err)
	t.Run("get existing log", func(t *testing.T) {
		for keyStr, log := range writeLogs {
			del, value := table.get([]byte(keyStr))
			assert.Equal(t, false, del)
			assert.Equal(t, log.Value, value)
		}
	})

	err = table.putBatch(deleteLogs, node.Generate(), writeOpts)
	assert.Nil(t, err)
	t.Run("get deleted log", func(t *testing.T) {
		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.Equal(t, true, del)
			assert.Equal(t, log.Value, value)
		}
	})

	err = table.close()
	assert.Nil(t, err)
}

func TestMemTableGetReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-get-reopen")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}

	writeOpts := &WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	assert.Nil(t, err)

	t.Run("get existing log reopen", func(t *testing.T) {
		table, err := openMemtable(opts)
		assert.Nil(t, err)
		writeLogs := map[string]*LogRecord{
			"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
			"":      {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
			"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
		}
		err = table.putBatch(writeLogs, node.Generate(), writeOpts)
		assert.Nil(t, err)
		err = table.close()
		assert.Nil(t, err)

		table, err = openMemtable(opts)
		assert.Nil(t, err)
		for keyStr, log := range writeLogs {
			del, value := table.get([]byte(keyStr))
			assert.Equal(t, false, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.Nil(t, err)
	})

	t.Run("get deleted log reopen", func(t *testing.T) {
		table, err := openMemtable(opts)
		assert.Nil(t, err)
		deleteLogs := map[string]*LogRecord{
			"key 0": {Key: []byte("key 0"), Value: []byte(""), Type: LogRecordDeleted},
			"":      {Key: nil, Value: []byte(""), Type: LogRecordDeleted},
			"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordDeleted},
		}
		err = table.putBatch(deleteLogs, node.Generate(), writeOpts)
		assert.Nil(t, err)

		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.Equal(t, true, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.Nil(t, err)

		table, err = openMemtable(opts)
		assert.Nil(t, err)
		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.Equal(t, true, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.Nil(t, err)
	})

}

func TestMemTableDelWal(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-delete-wal")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	table, err := openMemtable(opts)
	assert.Nil(t, err)

	t.Run("test memtable delete wal", func(t *testing.T) {
		err := table.deleteWAl()
		assert.Nil(t, err)
		entries, err := os.ReadDir(path)
		assert.Nil(t, err)
		assert.Equal(t, len(entries), 0)
	})

	err = table.close()
	assert.Nil(t, err)
}

func TestMemTableSync(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-sync")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	table, err := openMemtable(opts)
	assert.Nil(t, err)

	writeOpts := &WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	assert.Nil(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(i), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
	assert.Nil(t, err)

	t.Run("test memtable delete wal", func(t *testing.T) {
		err := table.sync()
		assert.Nil(t, err)
	})

	err = table.close()
	assert.Nil(t, err)
}

func TestMemtableClose(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-close")
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}

	table, err := openMemtable(opts)
	assert.Nil(t, err)

	t.Run("open memtable", func(t *testing.T) {
		err = table.close()
		assert.Nil(t, err)
	})
}
