package lotusdb

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func destroyDB(db *DB) {
	err := db.Close()
	if err != nil {
		panic(err)
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
			err = db.Put(tt.args.log.key, tt.args.log.value, WriteReliable)
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
		err := db.Put(log.key, log.value, WriteReliable)
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
		err := db.Put(log.key, log.value, WriteReliable)
		assert.Nil(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Delete(tt.args.log.key, WriteReliable)
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
		err := db.Put(log.key, log.value, WriteReliable)
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
		_ = db.Put(log.key, log.value, WriteReliable)
	}
	for i := 0; i < numLogs; i++ {
		// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
		log := &testLog{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
		_ = db.Put(log.key, log.value, WriteReliable)
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
		_ = db.Put(log.key, log.value, WriteReliable)
	}

	numLogs := 64
	var logs []*testLog
	for i := 0; i < numLogs; i++ {
		// the size of a logRecord is about 1MB (a little bigger than 1MB due to encode)
		log := &testLog{key: util.RandomValue(2 << 18), value: util.RandomValue(2 << 18)}
		logs = append(logs, log)
	}
	for _, log := range logs {
		_ = db.Put(log.key, log.value, WriteReliable)
	}
	for _, log := range logs {
		_ = db.Delete(log.key, WriteReliable)
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
	position, err := db.index.Get(key)
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
					_ = db.Put(log.key, log.value, WriteReliable)
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
					_ = db.Delete(log.key, WriteReliable)
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
