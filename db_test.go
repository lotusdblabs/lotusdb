package lotusdb

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/diskhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func destroyDB(db *DB) {
	if !db.closed {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}

	_ = os.RemoveAll(db.options.DirPath)
}

func TestDBOpen(t *testing.T) {
	// Save the original function
	t.Run("Valid options", func(t *testing.T) {
		options := DefaultOptions
		path, err := os.MkdirTemp("", "db-test-open")
		assert.Nil(t, err)
		options.DirPath = path
		db, err := Open(options)
		assert.Nil(t, err)
		defer destroyDB(db)

		assert.NotNil(t, db, "DB should not be nil")
		// Ensure that the properties of the DB are initialized correctly.
		assert.NotNil(t, db.activeMem, "DB activeMem should not be nil")
		assert.NotNil(t, db.index, "DB index should not be nil")
		assert.NotNil(t, db.vlog, "DB vlog should not be nil")
		assert.NotNil(t, db.fileLock, "DB fileLock should not be nil")
		assert.NotNil(t, db.flushChan, "DB flushChan should not be nil")

		err = db.Close()
		assert.Nil(t, err)
	})
	t.Run("Invalid options - no directory path", func(t *testing.T) {
		options := DefaultOptions
		options.DirPath = ""
		_, err := Open(options)
		require.Error(t, err, "Open should return an error")
	})
}

func TestDBClose(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-close")
	assert.Nil(t, err)
	options.DirPath = path
	db, err := Open(options)
	defer destroyDB(db)
	assert.Nil(t, err)
	t.Run("test close db", func(t *testing.T) {
		err := db.Close()
		assert.Nil(t, err)
	})
}

func TestDBSync(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-sync")
	assert.Nil(t, err)
	options.DirPath = path
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	t.Run("test sync db", func(t *testing.T) {
		err := db.Sync()
		assert.Nil(t, err)
	})
	err = db.Close()
	assert.Nil(t, err)
}

func TestDBPut(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-put")
	assert.Nil(t, err)
	options.DirPath = path

	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	type testLog struct {
		key   []byte
		value []byte
	}
	logs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: nil, value: []byte("value 2")},
		{key: []byte("key 3"), value: nil},
	}
	type args struct {
		log *testLog
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
			"nil key", args{log: logs[1]}, true,
		},
		{
			"nil value", args{log: logs[2]}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = db.Put(tt.args.log.key, tt.args.log.value, &WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}

	err = db.Close()
	assert.Nil(t, err)
}

func TestDBGet(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-get")
	assert.Nil(t, err)
	options.DirPath = path

	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	type testLog struct {
		key     []byte
		value   []byte
		wantErr bool
	}

	logs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0"), wantErr: false},
		{key: nil, value: []byte("value 2"), wantErr: true},
		{key: []byte("key 3"), value: nil, wantErr: false},
	}
	kv := map[string]string{
		"key 0": "value 0",
		"":      "",
		"key 3": "",
	}

	type args struct {
		log *testLog
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
			"nil key", args{log: logs[1]}, true,
		},
		{
			"nil value", args{log: logs[2]}, true,
		},
	}

	for _, log := range logs {
		err := db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		assert.Equal(t, err != nil, log.wantErr)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := db.Get(tt.args.log.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get(key) error = %v, wantErr = %v", err, tt.wantErr)
			}
			assert.Equal(t, kv[string(tt.args.log.key)], string(value))
		})
	}

	err = db.Close()
	assert.Nil(t, err)
}

func TestDBDelete(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-delete")
	assert.Nil(t, err)
	options.DirPath = path

	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	type testLog struct {
		key   []byte
		value []byte
	}

	logs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}

	type args struct {
		log *testLog
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"key-value 0", args{log: logs[0]}, false,
		},
		{
			"key-value 1", args{log: logs[1]}, false,
		},
		{
			"key-value 2", args{log: logs[2]}, false,
		},
	}

	for _, log := range logs {
		err := db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		assert.Nil(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Delete(tt.args.log.key, &WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("Get(key) error = %v, wantErr = %v", err, tt.wantErr)
			}
			value, err := db.Get(tt.args.log.key)
			assert.NotNil(t, err)
			assert.Equal(t, []byte(nil), value)
		})
	}

	err = db.Close()
	assert.Nil(t, err)
}

func TestDBExist(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-exist")
	assert.Nil(t, err)
	options.DirPath = path

	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	type testLog struct {
		key     []byte
		value   []byte
		wantErr bool
	}

	logs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0"), wantErr: false},
		{key: nil, value: []byte("value 2"), wantErr: true},
		{key: []byte("key 3"), value: nil, wantErr: false},
	}
	kv := map[string]bool{
		"key 0": true,
		"":      false,
		"key 3": false,
	}

	type args struct {
		log *testLog
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
			"nil key", args{log: logs[1]}, true,
		},
		{
			"nil value", args{log: logs[2]}, false,
		},
	}

	for _, log := range logs {
		err := db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		assert.Equal(t, err != nil, log.wantErr)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isExist, err := db.Exist(tt.args.log.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get(key) error = %v, wantErr = %v", err, tt.wantErr)
			}
			assert.Equal(t, kv[string(tt.args.log.key)], isExist)
		})
	}

	err = db.Close()
	assert.Nil(t, err)
}

func TestDBFlushMemTables(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-flush")
	assert.Nil(t, err)
	options.DirPath = path

	db, err := Open(options)
	assert.Nil(t, err)
	// defer destroyDB(db)

	type testLog struct {
		key   []byte
		value []byte
	}

	numLogs := 100
	logs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}
	for _, log := range logs {
		_ = db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}
	for i := 0; i < numLogs; i++ {
		// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
		log := &testLog{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
		_ = db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}

	t.Run("test flushMemtables", func(t *testing.T) {
		time.Sleep(time.Second * 1)
		for _, log := range logs {
			value, err := getValueFromVlog(db, log.key)
			assert.Nil(t, err)
			assert.Equal(t, log.value, value)
		}
	})

}

func TestDBCompact(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-compact")
	assert.Nil(t, err)
	options.DirPath = path
	options.CompactBatchCount = 2 << 5

	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	type testLog struct {
		key   []byte
		value []byte
	}

	testlogs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}
	for _, log := range testlogs {
		_ = db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}

	numLogs := 64
	var logs []*testLog
	for i := 0; i < numLogs; i++ {
		// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
		log := &testLog{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
		logs = append(logs, log)
	}
	for _, log := range logs {
		_ = db.Put(log.key, log.value, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}
	for _, log := range logs {
		_ = db.Delete(log.key, &WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}
	t.Run("test compaction", func(t *testing.T) {
		time.Sleep(time.Millisecond * 500)
		size, err := util.DirSize(db.options.DirPath)
		assert.Nil(t, err)

		err = db.Compact()
		assert.Nil(t, err)

		sizeCompact, err := util.DirSize(db.options.DirPath)
		assert.Nil(t, err)
		assert.Greater(t, size, sizeCompact)

		for _, log := range testlogs {
			value, err := getValueFromVlog(db, log.key)
			assert.Nil(t, err)
			assert.Equal(t, log.value, value)
		}
	})

}

func getValueFromVlog(db *DB, key []byte) ([]byte, error) {
	var value []byte
	var matchKey func(diskhash.Slot) (bool, error)
	if db.options.IndexType == Hash {
		matchKey = MatchKeyFunc(db, key, nil, &value)
	}
	position, err := db.index.Get(key, matchKey)
	if err != nil {
		return nil, err
	}
	if db.options.IndexType == Hash {
		if value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
	}
	if position == nil {
		return nil, ErrKeyNotFound
	}
	record, err := db.vlog.read(position)
	if err != nil {
		return nil, err
	}
	return record.value, nil
}

func TestDBMultiClients(t *testing.T) {
	type testLog struct {
		key   []byte
		value []byte
	}

	numLogs := 100
	var logs [][]*testLog
	for i := 0; i < 2; i++ {
		logs = append(logs, []*testLog{})
		for j := 0; j < numLogs; j++ {
			// the size of a logRecord is about 1kB (a little bigger than 1kB due to encode)
			log := &testLog{key: util.RandomValue(2 << 8), value: util.RandomValue(2 << 8)}
			logs[i] = append(logs[i], log)
		}
	}

	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-multi-client")
	assert.Nil(t, err)
	options.DirPath = path
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	t.Run("multi client running", func(t *testing.T) {
		var wg sync.WaitGroup

		// 2 clients to put
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				for _, log := range logs[i] {
					_ = db.Put(log.key, log.value, &WriteOptions{
						Sync:       true,
						DisableWal: false,
					})
					time.Sleep(time.Millisecond * 5)
				}
				wg.Done()
			}(i)
		}

		// 2 clients to get
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				for _, log := range logs[i] {
					_, _ = db.Get(log.key)
					time.Sleep(time.Millisecond * 5)
				}
				wg.Done()
			}(i)
		}

		// 2 clients to delete
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				for _, log := range logs[i] {
					_ = db.Delete(log.key, &WriteOptions{
						Sync:       true,
						DisableWal: false,
					})
					time.Sleep(time.Millisecond * 5)
				}
				wg.Done()
			}(i)
		}

		// 2 clients to exist
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				for _, log := range logs[i] {
					_, _ = db.Exist(log.key)
					time.Sleep(time.Millisecond * 5)
				}
				wg.Done()
			}(i)
		}

		wg.Wait()
	})
}

func TestDBIterator(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-iter")
	assert.Nil(t, err)
	options.DirPath = path
	db, err := Open(options)
	defer destroyDB(db)
	assert.Nil(t, err)
	db.immuMems = make([]*memtable, 3)
	opts := memtableOptions{
		dirPath:         path,
		tableId:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
		walBlockCache:   DefaultOptions.BlockCache,
	}
	for i := 0; i < 3; i++ {
		opts.tableId = uint32(i)
		db.immuMems[i], err = openMemtable(opts)
		assert.Nil(t, err)
	}
	logRecord_0 := []*LogRecord{
		// 0
		{[]byte("k3"), nil, LogRecordDeleted, 0},
		{[]byte("k1"), []byte("v1"), LogRecordNormal, 0},
		{[]byte("k1"), []byte("v1_1"), LogRecordNormal, 0},
		{[]byte("k2"), []byte("v1_1"), LogRecordNormal, 0},
		{[]byte("abc3"), nil, LogRecordDeleted, 0},
		{[]byte("abc1"), []byte("v1"), LogRecordNormal, 0},
		{[]byte("abc1"), []byte("v1_1"), LogRecordNormal, 0},
		{[]byte("abc2"), []byte("v1_1"), LogRecordNormal, 0},
	}
	logRecord_1 := []*LogRecord{
		{[]byte("k1"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("k2"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("k2"), []byte("v2_2"), LogRecordNormal, 0},
		{[]byte("abc1"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("abc2"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("abc2"), []byte("v2_2"), LogRecordNormal, 0},
	}
	logRecord_2 := []*LogRecord{
		// 2
		{[]byte("k2"), nil, LogRecordDeleted, 0},
		{[]byte("abc2"), nil, LogRecordDeleted, 0},
	}
	logRecord_3 := []*LogRecord{
		{[]byte("k3"), []byte("v3_1"), LogRecordNormal, 0},
		{[]byte("abc3"), []byte("v3_1"), LogRecordNormal, 0},
	}

	list2Map := func(in []*LogRecord) (out map[string]*LogRecord) {
		out = make(map[string]*LogRecord)
		for _, v := range in {
			out[string(v.Key)] = v
		}
		return
	}
	db.immuMems[0].putBatch(list2Map(logRecord_0), 0, nil)
	db.immuMems[1].putBatch(list2Map(logRecord_1), 1, nil)
	db.immuMems[2].putBatch(list2Map(logRecord_2), 2, nil)
	db.activeMem.putBatch(list2Map(logRecord_3), 3, nil)
	expectedKey := [][]byte{
		[]byte("k1"),
		[]byte("k3"),
	}
	expectedVal := [][]byte{
		[]byte("v2_1"),
		[]byte("v3_1"),
	}
	iter, err := db.NewIterator(IteratorOptions{
		Reverse: false,
		Prefix:  []byte("k"),
	})
	assert.Nil(t, err)
	var i int
	iter.Rewind()
	i = 0
	for iter.Valid() {
		if !iter.itrs[0].options.Reverse {
			assert.Equal(t, expectedKey[i], iter.Key())
			assert.Equal(t, expectedVal[i], iter.Value())

		} else {
			assert.Equal(t, expectedKey[2-i], iter.Key())
			assert.Equal(t, expectedVal[2-i], iter.Value())
		}
		i++
		iter.Next()
	}

	iter.Rewind()
	i = 0
	for iter.Valid() {
		if !iter.itrs[0].options.Reverse {
			assert.Equal(t, expectedKey[i], iter.Key())
			assert.Equal(t, expectedVal[i], iter.Value())

		} else {
			assert.Equal(t, expectedKey[2-i], iter.Key())
			assert.Equal(t, expectedVal[2-i], iter.Value())

		}
		i++
		iter.Next()
	}
	err = iter.Close()
	assert.Nil(t, err)

	iter, err = db.NewIterator(IteratorOptions{
		Reverse: true,
		Prefix:  []byte("k"),
	})
	assert.Nil(t, err)

	iter.Rewind()
	i = 0
	for iter.Valid() {
		if !iter.itrs[0].options.Reverse {
			assert.Equal(t, expectedKey[i], iter.Key())
			assert.Equal(t, expectedVal[i], iter.Value())

		} else {
			assert.Equal(t, expectedKey[1-i], iter.Key())
			assert.Equal(t, expectedVal[1-i], iter.Value())

		}
		i++
		iter.Next()
	}

	iter.Rewind()
	i = 0
	for iter.Valid() {
		if !iter.itrs[0].options.Reverse {
			assert.Equal(t, expectedKey[i], iter.Key())
			assert.Equal(t, expectedVal[i], iter.Value())

		} else {
			assert.Equal(t, expectedKey[1-i], iter.Key())
			assert.Equal(t, expectedVal[1-i], iter.Value())

		}
		i++
		iter.Next()
	}
	err = iter.Close()
	assert.Nil(t, err)

	for j := 0; j < 3; j++ {
		db.flushMemtable(db.immuMems[0])
		iter, err = db.NewIterator(IteratorOptions{
			Reverse: false,
			Prefix:  []byte("k"),
		})
		assert.Nil(t, err)

		iter.Rewind()
		i = 0
		for iter.Valid() {
			if !iter.itrs[0].options.Reverse {
				assert.Equal(t, expectedKey[i], iter.Key())
				assert.Equal(t, expectedVal[i], iter.Value())
			} else {
				assert.Equal(t, expectedKey[1-i], iter.Key())
				assert.Equal(t, expectedVal[1-i], iter.Value())
			}
			iter.Next()
			i++
		}
		err = iter.Close()
		assert.Nil(t, err)

		iter, err = db.NewIterator(IteratorOptions{
			Reverse: true,
			Prefix:  []byte("k"),
		})
		assert.Nil(t, err)

		iter.Rewind()
		i = 0
		for iter.Valid() {
			if !iter.itrs[0].options.Reverse {
				assert.Equal(t, expectedKey[i], iter.Key())
				assert.Equal(t, expectedVal[i], iter.Value())
			} else {
				assert.Equal(t, expectedKey[1-i], iter.Key())
				assert.Equal(t, expectedVal[1-i], iter.Value())
			}
			iter.Next()
			i++
		}
		err = iter.Close()
		assert.Nil(t, err)
	}
}
