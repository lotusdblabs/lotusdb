package lotusdb

import (
	"errors"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/gofrs/flock"
)

const (
	fileLockName = "FLOCK"
)

type DB struct {
	activeMem *memtable    // Active memtable for writing.
	immuMems  []*memtable  // Immutable memtables, waiting to be flushed to disk.
	index     Index        // index is multi-partition bptree to store key and chunk position.
	vlog      *valueLog    // vlog is the value log.
	fileLock  *flock.Flock // fileLock to prevent multiple processes from using the same database directory.
	flushChan chan *memtable
	mu        sync.RWMutex
	closed    bool
}

type Stat struct {
}

func Open(options Options) (*DB, error) {
	// banner

	// logger
	logPath := path.Join(DefaultLogDir, time.Now().Format("2006-01-02")+"_"+DefaultLogFileName)
	err := NewJSONLogger(
		WithInfoLevel(),
		WithFileRotationPath(logPath),
		WithDisableConsole(),
		WithTimeLayout("2006-01-02 15:04:05"),
		WithEnableHighlighting(),
	)
	if err != nil {
		Error("open db failed", zap.Error(err))
	}
	defer Sync()

	// check whether all options are valid
	if err := validateOptions(&options); err != nil {
		Error("db opens failed", zap.Error(err))
		return nil, err
	}

	// create the directory if not exist
	// create data directory if not exist
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			Error("create data directory failed", zap.Error(err))
			return nil, err
		}
		Info("create data directory success", zap.String("data path", options.DirPath))
	}

	// create file lock, prevent multiple processes from using the same database directory
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		Error("open db failed", zap.Error(err))
		return nil, err
	}
	if !hold {
		Error("open db failed", zap.Error(ErrDatabaseIsUsing))
		return nil, ErrDatabaseIsUsing
	}

	// open all memtables
	memtables, err := openAllMemtables(options)
	if err != nil {
		Error("open db failed", zap.Error(err))
		return nil, err
	}

	// open index
	index, err := openIndex(indexOptions{
		indexType:       indexBoltDB,
		dirPath:         options.DirPath,
		partitionNum:    options.PartitionNum,
		hashKeyFunction: options.KeyHashFunction,
	})
	if err != nil {
		Error("open db failed", zap.Error(err))
		return nil, err
	}

	// open value log
	vlog, err := openValueLog(valueLogOptions{
		dirPath:         options.DirPath,
		segmentSize:     options.ValueLogFileSize,
		blockCache:      options.BlockCache,
		partitionNum:    uint32(options.PartitionNum),
		hashKeyFunction: options.KeyHashFunction,
	})
	if err != nil {
		Error("open db failed", zap.Error(err))
		return nil, err
	}

	db := &DB{
		activeMem: memtables[len(memtables)-1],
		immuMems:  memtables[:len(memtables)-1],
		index:     index,
		vlog:      vlog,
		fileLock:  fileLock,
		flushChan: make(chan *memtable, options.MemtableNums-1),
	}

	// start flush memtables goroutine
	go db.flushMemtables()

	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// close all memtables
	for _, table := range db.immuMems {
		if err := table.close(); err != nil {
			Error("close memtables failed", zap.Error(err))
			return err
		}
	}
	if err := db.activeMem.close(); err != nil {
		Error("close memtables failed", zap.Error(err))
		return err
	}
	// close index
	if err := db.index.Close(); err != nil {
		Error("close index failed", zap.Error(err))
		return err
	}
	// close value log
	if err := db.vlog.close(); err != nil {
		Error("close value log failed", zap.Error(err))
		return err
	}
	// release file lock
	if err := db.fileLock.Unlock(); err != nil {
		Error("release file lock failed", zap.Error(err))
		return err
	}

	db.closed = true
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// sync all wal of memtables
	for _, table := range db.immuMems {
		if err := table.sync(); err != nil {
			Error("sync wal failed", zap.Error(err))
			return err
		}
	}
	if err := db.activeMem.sync(); err != nil {
		Error("sync wal failed", zap.Error(err))
		return err
	}
	// sync index
	if err := db.index.Sync(); err != nil {
		Error("sync index failed", zap.Error(err))
		return err
	}
	// sync value log
	if err := db.vlog.sync(); err != nil {
		Error("sync value log failed", zap.Error(err))
		return err
	}

	return nil
}

func (db *DB) Stat() *Stat {
	return nil
}

func (db *DB) Put(key []byte, value []byte, options *WriteOptions) error {
	batch := db.NewBatch(BatchOptions{
		Sync:     false,
		ReadOnly: false,
	})
	if err := batch.Put(key, value); err != nil {
		return err
	}
	return batch.Commit(options)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.NewBatch(BatchOptions{
		Sync:     false,
		ReadOnly: true,
	})
	defer func() {
		_ = batch.Commit(nil)
	}()
	return batch.Get(key)
}

func (db *DB) Delete(key []byte, options *WriteOptions) error {
	batch := db.NewBatch(BatchOptions{
		Sync:     false,
		ReadOnly: false,
	})
	if err := batch.Delete(key); err != nil {
		return err
	}
	return batch.Commit(options)
}

func (db *DB) Exist(key []byte) (bool, error) {
	batch := db.NewBatch(BatchOptions{
		Sync:     false,
		ReadOnly: true,
	})
	defer func() {
		_ = batch.Commit(nil)
	}()
	return batch.Exist(key)
}

// validateOptions validates the given options.
func validateOptions(options *Options) error {
	if options.DirPath == "" {
		return errors.New("dir path is empty")
	}
	if options.MemtableSize <= 0 {
		options.MemtableSize = 64 << 20 // 64MB
	}
	if options.MemtableNums <= 0 {
		options.MemtableNums = 15
	}
	if options.PartitionNum <= 0 {
		options.PartitionNum = 5
	}
	if options.ValueLogFileSize <= 0 {
		options.ValueLogFileSize = 1 << 30 // 1GB
	}
	return nil
}

func (db *DB) getMemTables() []*memtable {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var tables []*memtable
	tables = append(tables, db.activeMem)

	last := len(db.immuMems) - 1
	for i := range db.immuMems {
		tables = append(tables, db.immuMems[last-i])
	}

	return tables
}

func (db *DB) waitMemetableSpace() error {
	if !db.activeMem.isFull() {
		return nil
	}

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case db.flushChan <- db.activeMem:
		db.immuMems = append(db.immuMems, db.activeMem)
		options := db.activeMem.options
		options.tableId++
		table, err := openMemtable(options)
		if err != nil {
			return err
		}
		db.activeMem = table
	case <-timer.C:
		return errors.New("wait memtable space to write failed")
	}

	return nil
}

func (db *DB) flushMemtables() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case table := <-db.flushChan:
			sklIter := table.skl.NewIterator()
			var deletedKeys [][]byte
			var logRecords []*ValueLogRecord
			for sklIter.SeekToFirst(); sklIter.Valid(); sklIter.Next() {
				key, valueStruct := y.ParseKey(sklIter.Key()), sklIter.Value()
				if valueStruct.Meta == LogRecordDeleted {
					deletedKeys = append(deletedKeys, key)
				} else {
					logRecord := ValueLogRecord{key: key, value: valueStruct.Value}
					logRecords = append(logRecords, &logRecord)
				}
			}
			_ = sklIter.Close()

			// write to value log, get the positions of keys
			keyPos, err := db.vlog.writeBatch(logRecords)
			if err != nil {
				Error("vlog writeBatch failed:", zap.Error(err))
				continue
			}
			// sync the value log
			if err := db.vlog.sync(); err != nil {
				Error("vlog sync failed:", zap.Error(err))
				continue
			}

			// write all keys and positions to index
			if err := db.index.PutBatch(keyPos); err != nil {
				Error("index PutBatch failed:", zap.Error(err))
				continue
			}
			if err := db.index.DeleteBatch(deletedKeys); err != nil {
				Error("index DeleteBatch failed:", zap.Error(err))
				continue
			}
			// sync the index
			if err := db.index.Sync(); err != nil {
				Error("index sync failed:", zap.Error(err))
				continue
			}

			// delete the wal
			if err := table.deleteWAl(); err != nil {
				Error("delete wal failed:", zap.Error(err))
				continue
			}

			// delete old memtable keeped in memory
			db.mu.Lock()
			if len(db.immuMems) == 1 {
				db.immuMems = db.immuMems[:0]
			} else {
				db.immuMems = db.immuMems[1:]
			}
			db.mu.Unlock()

		case <-sig:
			return
		}
	}
}
