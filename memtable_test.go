package lotusdb

import (
	"bytes"
	"io/fs"
	"os"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtableOpen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-open")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	t.Run("open memtable", func(t *testing.T) {
		var table *memtable
		table, err = openMemtable(opts)
		assert.NotNil(t, table)
		require.NoError(t, err)
		err = table.close()
		assert.NoError(t, err)
	})
}

func TestMemtableOpenAll(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-open-all")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	var table *memtable
	for i := 0; i < DefaultOptions.MemtableNums; i++ {
		opts := memtableOptions{
			dirPath:         path,
			tableID:         uint32(i),
			memSize:         DefaultOptions.MemtableSize,
			walBytesPerSync: DefaultOptions.BytesPerSync,
			walSync:         DefaultBatchOptions.Sync,
		}
		table, err = openMemtable(opts)
		require.NoError(t, err)
		assert.NotNil(t, table)
		err = table.close()
		require.NoError(t, err)
	}

	t.Run("test open all memtables", func(t *testing.T) {
		var tables []*memtable
		var opts = DefaultOptions
		opts.DirPath = path
		tables, err = openAllMemtables(opts)
		require.NoError(t, err)
		assert.NotNil(t, tables)
		for _, table := range tables {
			err = table.close()
			assert.NoError(t, err)
		}
	})
}

func TestMemTablePutAllKindsEntries(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-all-kinds-entries")
	require.NoError(t, err)
	assert.NotEqual(t, "", path)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

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
			err = table.putBatch(tt.args.entry, node.Generate(), writeOpts)
			assert.NoError(t, err)
		})
	}

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTablePutBatch(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-batch")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(int64(i)), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	t.Run("test memory table put batch", func(t *testing.T) {
		err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
		assert.NoError(t, err)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTablePutBatchReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-batch-reopen")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(int64(i)), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
	require.NoError(t, err)

	t.Run("test memory table put batch after reopening", func(t *testing.T) {
		err = table.close()
		require.NoError(t, err)
		table, err = openMemtable(opts)
		require.NoError(t, err)
		err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
		require.NoError(t, err)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTableGet(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-get")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

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
	require.NoError(t, err)
	t.Run("get existing log", func(t *testing.T) {
		for keyStr, log := range writeLogs {
			del, value := table.get([]byte(keyStr))
			assert.False(t, del)
			assert.Equal(t, value, log.Value)
		}
	})

	err = table.putBatch(deleteLogs, node.Generate(), writeOpts)
	require.NoError(t, err)
	t.Run("get deleted log", func(t *testing.T) {
		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.True(t, del)
			assert.Equal(t, value, log.Value)
		}
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTableGetReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-get-reopen")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)
	t.Run("get existing log reopen", func(t *testing.T) {
		var table *memtable
		table, err = openMemtable(opts)
		require.NoError(t, err)
		writeLogs := map[string]*LogRecord{
			"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
			"":      {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
			"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
		}
		err = table.putBatch(writeLogs, node.Generate(), writeOpts)
		require.NoError(t, err)
		err = table.close()
		require.NoError(t, err)

		table, err = openMemtable(opts)
		require.NoError(t, err)
		for keyStr, log := range writeLogs {
			del, value := table.get([]byte(keyStr))
			assert.False(t, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.NoError(t, err)
	})
	t.Run("get deleted log reopen", func(t *testing.T) {
		var table *memtable
		table, err = openMemtable(opts)
		require.NoError(t, err)
		deleteLogs := map[string]*LogRecord{
			"key 0": {Key: []byte("key 0"), Value: []byte(""), Type: LogRecordDeleted},
			"":      {Key: nil, Value: []byte(""), Type: LogRecordDeleted},
			"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordDeleted},
		}
		err = table.putBatch(deleteLogs, node.Generate(), writeOpts)
		require.NoError(t, err)

		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.True(t, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		require.NoError(t, err)

		table, err = openMemtable(opts)
		require.NoError(t, err)
		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.True(t, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.NoError(t, err)
	})
}

func TestMemTableDelWal(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-delete-wal")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	t.Run("test memtable delete wal", func(t *testing.T) {
		var entries []fs.DirEntry
		err = table.deleteWAl()
		require.NoError(t, err)
		entries, err = os.ReadDir(path)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTableSync(t *testing.T) {
	var err error
	path, err := os.MkdirTemp("", "memtable-test-sync")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(int64(i)), Value: val}
		pendingWrites[string(log.Key)] = log
	}
	err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
	require.NoError(t, err)

	t.Run("test memtable delete wal", func(t *testing.T) {
		err = table.sync()
		assert.NoError(t, err)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemtableClose(t *testing.T) {
	var err error
	path, err := os.MkdirTemp("", "memtable-test-close")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	table, err := openMemtable(opts)
	require.NoError(t, err)

	t.Run("open memtable", func(t *testing.T) {
		err = table.close()
		assert.NoError(t, err)
	})
}

func TestNewMemtableIterator(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-iterator-new")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	table, err := openMemtable(opts)
	defer func() {
		err = table.close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)

	options := IteratorOptions{
		Reverse: false,
	}
	iter := newMemtableIterator(options, table)
	require.NoError(t, err)

	err = iter.Close()
	assert.NoError(t, err)
}

func Test_memtableIterator(t *testing.T) {
	var err error
	path, err := os.MkdirTemp("", "memtable-test-iterator-rewind")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)
	writeLogs := map[string]*LogRecord{
		"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		"key 1": {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
		"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
	}
	writeLogs2 := map[string]*LogRecord{
		"abc 0": {Key: []byte("abc 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		"key 3": {Key: nil, Value: []byte("key 3"), Type: LogRecordNormal},
		"abc 1": {Key: []byte("abc 1"), Value: []byte(""), Type: LogRecordNormal},
	}
	err = table.putBatch(writeLogs, node.Generate(), writeOpts)
	require.NoError(t, err)

	iteratorOptions := IteratorOptions{
		Reverse: false,
	}
	itr := newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	var prev []byte
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == -1)
		assert.Equal(t, writeLogs[string(currKey)].Value, itr.Value().(y.ValueStruct).Value)
		assert.Equal(t, writeLogs[string(currKey)].Type, itr.Value().(y.ValueStruct).Meta)
		prev = currKey
		itr.Next()
	}
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = true
	prev = nil
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == 1)
		prev = currKey
		assert.Equal(t, writeLogs[string(currKey)].Value, itr.Value().(y.ValueStruct).Value)
		assert.Equal(t, writeLogs[string(currKey)].Type, itr.Value().(y.ValueStruct).Meta)
		itr.Next()
	}
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = false
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Seek([]byte("key 0"))
	assert.Equal(t, []byte("key 0"), itr.Key())
	itr.Seek([]byte("key 4"))
	assert.False(t, itr.Valid())

	itr.Seek([]byte("aye 2"))
	assert.Equal(t, []byte("key 0"), itr.Key())
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = true
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Seek([]byte("key 4"))
	assert.Equal(t, []byte("key 2"), itr.Key())

	itr.Seek([]byte("key 2"))
	assert.Equal(t, []byte("key 2"), itr.Key())

	itr.Seek([]byte("aye 2"))
	assert.False(t, itr.Valid())

	err = itr.Close()
	require.NoError(t, err)

	// prefix
	err = table.putBatch(writeLogs2, node.Generate(), writeOpts)
	require.NoError(t, err)

	iteratorOptions.Reverse = false
	iteratorOptions.Prefix = []byte("not valid")
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Rewind()
	assert.False(t, itr.Valid())
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = false
	iteratorOptions.Prefix = []byte("abc")
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Rewind()
	for itr.Valid() {
		assert.True(t, bytes.HasPrefix(itr.Key(), iteratorOptions.Prefix))
		assert.Equal(t, writeLogs2[string(itr.Key())].Value, itr.Value().(y.ValueStruct).Value)
		itr.Next()
	}
	err = itr.Close()
	assert.NoError(t, err)
}
