package lotusdb

import (
	"os"
	"testing"
	"time"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBOpen(t *testing.T) {
	// Save the original function
	t.Run("Valid options", func(t *testing.T) {
		options := DefaultOptions
		path, err := os.MkdirTemp("", "db-test-open")
		assert.Nil(t, err)
		options.DirPath = path
		defer os.RemoveAll(options.DirPath)
		db, err := Open(options)
		assert.Nil(t, err)

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

	defer os.RemoveAll(options.DirPath)

	db, err := Open(options)
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

	defer os.RemoveAll(options.DirPath)

	db, err := Open(options)
	assert.Nil(t, err)

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

	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.Nil(t, err)

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

	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.Nil(t, err)

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

	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.Nil(t, err)

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

	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.Nil(t, err)

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

	defer os.RemoveAll(options.DirPath)
	db, err := Open(options)
	assert.Nil(t, err)

	type testLog struct {
		key   []byte
		value []byte
	}

	numLogs := 2 << 9 * 100
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
	for i := 0; i < int(numLogs); i++ {
		// the size of a logRecord is about 1kB (a little bigger than 1kB due to encode)
		log := &testLog{key: util.RandomValue(2 << 8), value: util.RandomValue(2 << 8)}
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

	numLogs := 2 << 9 * 50
	var logs [][]*testLog
	for i := 0; i < 2; i++ {
		logs = append(logs, []*testLog{})
		for j := 0; j < int(numLogs); j++ {
			// the size of a logRecord is about 1kB (a little bigger than 1kB due to encode)
			log := &testLog{key: util.RandomValue(2 << 8), value: util.RandomValue(2 << 8)}
			logs[i] = append(logs[i], log)
		}
	}

	path, err := os.MkdirTemp("", "db-test-client")
	defer os.RemoveAll(path)

	t.Run("multi client run", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			go func(i int) {
				options := DefaultOptions
				assert.Nil(t, err)
				options.DirPath = path
				dbCliPut, err := Open(options)
				assert.Nil(t, err)

				for _, log := range logs[i] {
					_ = dbCliPut.Put(log.key, log.value, &WriteOptions{
						Sync:       true,
						DisableWal: false,
					})
				}

				err = dbCliPut.Close()
				assert.Nil(t, err)
			}(i)
		}

		for i := 0; i < 2; i++ {
			go func(i int) {
				options := DefaultOptions
				assert.Nil(t, err)
				options.DirPath = path
				dbCliGet, err := Open(options)
				assert.Nil(t, err)

				for _, log := range logs[i] {
					_, _ = dbCliGet.Get(log.key)
				}

				err = dbCliGet.Close()
				assert.Nil(t, err)
			}(i)
		}

		for i := 0; i < 2; i++ {
			go func(i int) {
				options := DefaultOptions
				assert.Nil(t, err)
				options.DirPath = path
				dbCliDel, err := Open(options)
				assert.Nil(t, err)

				for _, log := range logs[i] {
					_ = dbCliDel.Delete(log.key, &WriteOptions{
						Sync:       true,
						DisableWal: false,
					})
				}

				err = dbCliDel.Close()
				assert.Nil(t, err)
			}(i)
		}
	})
}
