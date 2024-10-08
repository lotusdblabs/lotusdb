package lotusdb

import (
	"bytes"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/diskhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLog struct {
	key   []byte
	value []byte
}

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
		require.NoError(t, err)
		assert.NotEqual(t, "", path)
		options.DirPath = path
		db, err := Open(options)
		require.NoError(t, err)
		defer destroyDB(db)

		assert.NotNil(t, db, "DB should not be nil")
		// Ensure that the properties of the DB are initialized correctly.
		assert.NotNil(t, db.activeMem, "DB activeMem should not be nil")
		assert.NotNil(t, db.index, "DB index should not be nil")
		assert.NotNil(t, db.vlog, "DB vlog should not be nil")
		assert.NotNil(t, db.fileLock, "DB fileLock should not be nil")
		assert.NotNil(t, db.flushChan, "DB flushChan should not be nil")

		err = db.Close()
		assert.NoError(t, err)
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
	require.NoError(t, err)
	assert.NotEqual(t, "", path)
	options.DirPath = path
	db, err := Open(options)
	defer destroyDB(db)
	require.NoError(t, err)
	t.Run("test close db", func(t *testing.T) {
		err = db.Close()
		assert.NoError(t, err)
	})
}

func TestDBSync(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-sync")
	require.NoError(t, err)
	options.DirPath = path
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	t.Run("test sync db", func(t *testing.T) {
		err = db.Sync()
		assert.NoError(t, err)
	})
	err = db.Close()
	assert.NoError(t, err)
}

func TestDBPut(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-put")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
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
			err = db.PutWithOptions(tt.args.log.key, tt.args.log.value, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBatch() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestDBGet(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-get")
	if assert.NoError(t, err) {
		assert.NotEqual(t, "", path)
	}
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
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
		errPutWithOptions := db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		assert.Equal(t, log.wantErr, errPutWithOptions != nil)
	}
	var value []byte
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err = db.Get(tt.args.log.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get(key) error = %v, wantErr = %v", err, tt.wantErr)
			}
			assert.Equal(t, kv[string(tt.args.log.key)], string(value))
		})
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestDBDelete(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-delete")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
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
		errPutWithOptions := db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		require.NoError(t, errPutWithOptions)
	}

	var value []byte
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errDeleteWithOptions := db.DeleteWithOptions(tt.args.log.key, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
			if (errDeleteWithOptions != nil) != tt.wantErr {
				t.Errorf("Get(key) error = %v, wantErr = %v", err, tt.wantErr)
			}
			value, err = db.Get(tt.args.log.key)
			require.Error(t, err)
			assert.Equal(t, []byte(nil), value)
		})
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestDBExist(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-exist")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
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
		errPutWithOptions := db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
		assert.Equal(t, log.wantErr, errPutWithOptions != nil)
	}

	var isExist bool
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isExist, err = db.Exist(tt.args.log.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get(key) error = %v, wantErr = %v", err, tt.wantErr)
			}
			assert.Equal(t, kv[string(tt.args.log.key)], isExist)
		})
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestDBFlushMemTables(t *testing.T) {
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-flush")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

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
		_ = db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}

	delLogs := []*testLog{
		{key: []byte("key 3"), value: []byte("value 3")},
	}
	for _, log := range delLogs {
		_ = db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}
	for i := 0; i < numLogs; i++ {
		// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
		log := &testLog{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
		_ = db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}

	t.Run("test flushMemtables", func(t *testing.T) {
		time.Sleep(time.Second * 1)
		var value []byte
		for _, log := range logs {
			value, err = getValueFromVlog(db, log.key)
			require.NoError(t, err)
			assert.Equal(t, log.value, value)
		}

		for _, log := range delLogs {
			partition := db.vlog.getKeyPartition(log.key)
			record, _ := getRecordFromVlog(db, log.key)
			_ = db.DeleteWithOptions(log.key, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
			for i := 0; i < numLogs; i++ {
				// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
				tlog := &testLog{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
				_ = db.PutWithOptions(tlog.key, tlog.value, WriteOptions{
					Sync:       true,
					DisableWal: false,
				})
			}
			time.Sleep(1 * time.Second)
			assert.True(t, true, db.vlog.dpTables[partition].existEntry(record.uid))
		}
	})
}

func TestDBCompact(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = false
	path, err := os.MkdirTemp("", "db-test-compact")
	require.NoError(t, err)
	options.DirPath = path
	options.CompactBatchCapacity = 1 << 6

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	testlogs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}

	for _, log := range testlogs {
		_ = db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}

	produceAndWriteLogs(100000, 0, db)
	// overwrite half. background busy flushing.
	produceAndWriteLogs(50000, 0, db)
	produceAndWriteLogs(50000, 0, db)
	produceAndWriteLogs(50000, 0, db)
	produceAndWriteLogs(50000, 0, db)

	t.Run("test compaction", func(t *testing.T) {
		var size, sizeCompact int64

		size, err = util.DirSize(db.options.DirPath)
		require.NoError(t, err)

		err = db.Compact()
		require.NoError(t, err)

		sizeCompact, err = util.DirSize(db.options.DirPath)
		require.NoError(t, err)
		require.Greater(t, size, sizeCompact)
		var value []byte
		for _, log := range testlogs {
			value, err = getValueFromVlog(db, log.key)
			require.NoError(t, err)
			assert.Equal(t, log.value, value)
		}
	})
}

func TestDBCompactWitchDeprecatetable(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = false
	path, err := os.MkdirTemp("", "db-test-CompactWitchDeprecatetable")
	require.NoError(t, err)
	options.DirPath = path
	options.CompactBatchCapacity = 1 << 6

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	testlogs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}

	for _, log := range testlogs {
		_ = db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}

	produceAndWriteLogs(100000, 0, db)
	// overwrite half. background busy flushing.
	produceAndWriteLogs(50000, 0, db)
	produceAndWriteLogs(50000, 0, db)
	produceAndWriteLogs(50000, 0, db)
	produceAndWriteLogs(50000, 0, db)

	t.Run("test compaction", func(t *testing.T) {
		var size, sizeCompact int64

		size, err = util.DirSize(db.options.DirPath)
		require.NoError(t, err)

		err = db.CompactWithDeprecatedtable()
		require.NoError(t, err)

		sizeCompact, err = util.DirSize(db.options.DirPath)
		require.NoError(t, err)
		require.Greater(t, size, sizeCompact)
		var value []byte
		for _, log := range testlogs {
			value, err = getValueFromVlog(db, log.key)
			require.NoError(t, err)
			assert.Equal(t, log.value, value)
		}
	})
}

func TestDBAutoCompact(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = true
	path, err := os.MkdirTemp("", "db-test-AutoCompact")
	require.NoError(t, err)
	options.DirPath = path
	options.CompactBatchCapacity = 1 << 6

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	testlogs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}

	testrmlogs := []*testLog{
		{key: []byte("key 0 rm"), value: []byte("value 0")},
		{key: []byte("key 1 rm"), value: []byte("value 1")},
		{key: []byte("key 2 rm"), value: []byte("value 2")},
	}

	t.Run("test compaction", func(t *testing.T) {
		for _, log := range testlogs {
			_ = db.PutWithOptions(log.key, log.value, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
		}
		for _, log := range testrmlogs {
			_ = db.DeleteWithOptions(log.key, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
		}
		// load init key value.
		produceAndWriteLogs(100000, 0, db)
		// overwrite half. background busy flushing.
		for i := 0; i < 6; i++ {
			time.Sleep(500 * time.Microsecond)
			produceAndWriteLogs(50000, 0, db)
		}

		require.NoError(t, err)

		var value []byte
		for _, log := range testlogs {
			value, err = getValueFromVlog(db, log.key)
			require.NoError(t, err)
			assert.Equal(t, log.value, value)
		}
		for _, log := range testrmlogs {
			value, err = db.Get(log.key)
			require.Error(t, err)
			assert.Equal(t, []byte(nil), value)
		}
	})
}

func TestDBAutoCompactWithBusyIO(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = true
	options.AdvisedCompactionRate = 0.2
	options.ForceCompactionRate = 0.5
	path, err := os.MkdirTemp("", "db-test-AutoCompactWithBusyIO")
	require.NoError(t, err)
	options.DirPath = path
	options.CompactBatchCapacity = 1 << 6

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	testlogs := []*testLog{
		{key: []byte("key 0"), value: []byte("value 0")},
		{key: []byte("key 1"), value: []byte("value 1")},
		{key: []byte("key 2"), value: []byte("value 2")},
	}

	testrmlogs := []*testLog{
		{key: []byte("key 0 rm"), value: []byte("value 0")},
		{key: []byte("key 1 rm"), value: []byte("value 1")},
		{key: []byte("key 2 rm"), value: []byte("value 2")},
	}

	t.Run("test compaction", func(t *testing.T) {
		for _, log := range testlogs {
			_ = db.PutWithOptions(log.key, log.value, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
		}
		for _, log := range testrmlogs {
			_ = db.DeleteWithOptions(log.key, WriteOptions{
				Sync:       true,
				DisableWal: false,
			})
		}
		// load init key value.
		ioCloseChan := make(chan struct{})
		go func() {
			SimpleIO(options.DirPath+"iofile", 10)
			ioCloseChan <- struct{}{}
		}()

		produceAndWriteLogs(100000, 0, db)
		// overwrite half. background busy flushing.
		produceAndWriteLogs(50000, 0, db)
		go SimpleIO(options.DirPath+"iofile", 10)
		produceAndWriteLogs(50000, 0, db)
		produceAndWriteLogs(50000, 0, db)
		produceAndWriteLogs(50000, 0, db)
		// we sleep 1s, this time not IO busy. So that background will do autoCompact.
		time.Sleep(1 * time.Second)
		<-ioCloseChan
		close(ioCloseChan)
		produceAndWriteLogs(50000, 0, db)
		produceAndWriteLogs(50000, 0, db)
		require.NoError(t, err)

		var value []byte
		for _, log := range testlogs {
			value, err = getValueFromVlog(db, log.key)
			require.NoError(t, err)
			assert.Equal(t, log.value, value)
		}
		for _, log := range testrmlogs {
			value, err = db.Get(log.key)
			require.Error(t, err)
			assert.Equal(t, []byte(nil), value)
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

func produceAndWriteLogs(numLogs int64, offset int64, db *DB) []*testLog {
	var logs []*testLog
	var i int64
	for i = 0; i < numLogs; i++ {
		// the size of a logRecord is about 1KB (a little bigger than 1KB due to encode)
		log := &testLog{key: util.GetTestKey(offset + i), value: util.RandomValue(1 << 10)}
		logs = append(logs, log)
	}
	for _, log := range logs {
		_ = db.PutWithOptions(log.key, log.value, WriteOptions{
			Sync:       true,
			DisableWal: false,
		})
	}
	return logs
}

func getRecordFromVlog(db *DB, key []byte) (*ValueLogRecord, error) {
	var value []byte
	var matchKey func(diskhash.Slot) (bool, error)
	if db.options.IndexType == Hash {
		matchKey = MatchKeyFunc(db, key, nil, &value)
	}
	position, err := db.index.Get(key, matchKey)
	if err != nil {
		return nil, err
	}
	if position == nil {
		return nil, ErrKeyNotFound
	}
	record, err := db.vlog.read(position)
	if err != nil {
		return nil, err
	}
	return record, nil
}

//nolint:gocognit
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
	require.NoError(t, err)
	options.DirPath = path
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	t.Run("multi client running", func(_ *testing.T) {
		var wg sync.WaitGroup

		// 2 clients to put
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				delLogs := produceAndWriteLogs(50000, int64(i)*50000, db)
				// delete logs
				for idx, log := range delLogs {
					if idx%5 == 0 {
						_ = db.DeleteWithOptions(log.key, WriteOptions{
							Sync:       true,
							DisableWal: false,
						})
					}
				}
				produceAndWriteLogs(50000, int64(i)*50000, db)
				time.Sleep(time.Millisecond * 500)
				for _, log := range logs[i] {
					_ = db.PutWithOptions(log.key, log.value, WriteOptions{
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
					_ = db.DeleteWithOptions(log.key, WriteOptions{
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

//nolint:gocognit
func TestDBIterator(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = false
	path, err := os.MkdirTemp("", "db-test-iter")
	require.NoError(t, err)
	options.DirPath = path
	db, err := Open(options)
	defer destroyDB(db)
	require.NoError(t, err)
	db.immuMems = make([]*memtable, 3)
	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	for i := 0; i < 3; i++ {
		opts.tableID = uint32(i)
		db.immuMems[i], err = openMemtable(opts)
		require.NoError(t, err)
	}
	logRecord0 := []*LogRecord{
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
	logRecord1 := []*LogRecord{
		{[]byte("k1"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("k2"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("k2"), []byte("v2_2"), LogRecordNormal, 0},
		{[]byte("abc1"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("abc2"), []byte("v2_1"), LogRecordNormal, 0},
		{[]byte("abc2"), []byte("v2_2"), LogRecordNormal, 0},
	}
	logRecord2 := []*LogRecord{
		// 2
		{[]byte("k2"), nil, LogRecordDeleted, 0},
		{[]byte("abc2"), nil, LogRecordDeleted, 0},
	}
	logRecord3 := []*LogRecord{
		{[]byte("k3"), []byte("v3_1"), LogRecordNormal, 0},
		{[]byte("abc3"), []byte("v3_1"), LogRecordNormal, 0},
	}

	list2Map := func(in []*LogRecord) map[string]*LogRecord {
		out := make(map[string]*LogRecord)
		for _, v := range in {
			out[string(v.Key)] = v
		}
		return out
	}
	err = db.immuMems[0].putBatch(list2Map(logRecord0), 0, DefaultWriteOptions)
	require.NoError(t, err)
	err = db.immuMems[1].putBatch(list2Map(logRecord1), 1, DefaultWriteOptions)
	require.NoError(t, err)
	err = db.immuMems[2].putBatch(list2Map(logRecord2), 2, DefaultWriteOptions)
	require.NoError(t, err)
	err = db.activeMem.putBatch(list2Map(logRecord3), 3, DefaultWriteOptions)
	require.NoError(t, err)

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
	require.NoError(t, err)
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
	require.NoError(t, err)

	iter, err = db.NewIterator(IteratorOptions{
		Reverse: true,
		Prefix:  []byte("k"),
	})
	require.NoError(t, err)

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
	require.NoError(t, err)

	for j := 0; j < 3; j++ {
		db.flushMemtable(db.immuMems[0])
		iter, err = db.NewIterator(IteratorOptions{
			Reverse: false,
			Prefix:  []byte("k"),
		})
		require.NoError(t, err)

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
		require.NoError(t, err)

		iter, err = db.NewIterator(IteratorOptions{
			Reverse: true,
			Prefix:  []byte("k"),
		})
		require.NoError(t, err)

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
		require.NoError(t, err)
	}

	iter, err = db.NewIterator(IteratorOptions{
		Reverse: false,
		Prefix:  []byte("k"),
	})
	require.NoError(t, err)

	iter.Seek([]byte("k3"))
	var prev []byte
	for iter.Valid() {
		assert.True(t, prev == nil || bytes.Compare(prev, iter.Key()) == -1)
		assert.True(t, bytes.HasPrefix(iter.Key(), []byte("k3")))
		prev = iter.Key()
		iter.Next()
	}
	err = iter.Close()
	require.NoError(t, err)

	// unsupported type
	options = DefaultOptions
	options.IndexType = Hash
	db, err = Open(options)
	require.NoError(t, err)
	itr, err := db.NewIterator(IteratorOptions{Reverse: false})
	assert.Equal(t, ErrDBIteratorUnsupportedTypeHASH, err)
	assert.Nil(t, itr)
}

func TestDeprecatetableMetaPersist(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = true
	path, err := os.MkdirTemp("", "db-test-DeprecatetableMetaPersist")
	require.NoError(t, err)
	options.DirPath = path
	options.CompactBatchCapacity = 1 << 6

	db, err := Open(options)
	require.NoError(t, err)

	t.Run("test same deprecated number", func(t *testing.T) {
		produceAndWriteLogs(100000, 0, db)
		// overwrite half. background busy flushing.
		for i := 0; i < 3; i++ {
			time.Sleep(500 * time.Microsecond)
			produceAndWriteLogs(50000, 0, db)
		}
		db.Close()
		deprecatedNumberFirst := db.vlog.deprecatedNumber
		totalNumberFirst := db.vlog.totalNumber
		db, err = Open(options)
		deprecatedNumberSecond := db.vlog.deprecatedNumber
		totalNumberSecond := db.vlog.totalNumber
		require.NoError(t, err)
		assert.Equal(t, deprecatedNumberFirst, deprecatedNumberSecond)
		assert.Equal(t, totalNumberFirst, totalNumberSecond)
	})
}

func SimpleIO(targetPath string, count int) {
	file, err := os.Create(targetPath)
	if err != nil {
		log.Println("Error creating file:", err)
		return
	}
	data := util.RandomValue(1 << 24)
	for count > 0 {
		count--
		_, err = file.Write(data)
		if err != nil {
			log.Println("Error writing to file:", err)
			return
		}

		err = file.Sync()
		if err != nil {
			log.Println("Error syncing file:", err)
			return
		}

		time.Sleep(1 * time.Millisecond)
	}
}
