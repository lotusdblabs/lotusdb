package lotusdb

import (
	"errors"
	"log"
	"os"
	"os/signal"
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
	options   Options
	batchPool sync.Pool
}

func Open(options Options) (*DB, error) {
	// check whether all options are valid
	if err := validateOptions(&options); err != nil {
		return nil, err
	}

	// create data directory if not exist
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create file lock, prevent multiple processes from using the same database directory
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// open all memtables
	memtables, err := openAllMemtables(options)
	if err != nil {
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
		return nil, err
	}

	db := &DB{
		activeMem: memtables[len(memtables)-1],
		immuMems:  memtables[:len(memtables)-1],
		index:     index,
		vlog:      vlog,
		fileLock:  fileLock,
		flushChan: make(chan *memtable, options.MemtableNums-1),
		options:   options,
		batchPool: sync.Pool{New: makeBatch},
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
			return err
		}
	}
	if err := db.activeMem.close(); err != nil {
		return err
	}
	// close index
	if err := db.index.Close(); err != nil {
		return err
	}
	// close value log
	if err := db.vlog.close(); err != nil {
		return err
	}
	// release file lock
	if err := db.fileLock.Unlock(); err != nil {
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
			return err
		}
	}
	if err := db.activeMem.sync(); err != nil {
		return err
	}
	// sync index
	if err := db.index.Sync(); err != nil {
		return err
	}
	// sync value log
	if err := db.vlog.sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Put(key []byte, value []byte, options *WriteOptions) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db).withPendingWrites()
	if err := batch.Put(key, value); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit(options)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit(nil)
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

func (db *DB) Delete(key []byte, options *WriteOptions) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db).withPendingWrites()
	if err := batch.Delete(key); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit(options)
}

func (db *DB) Exist(key []byte) (bool, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit(nil)
		batch.reset()
		db.batchPool.Put(batch)
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
				log.Println("vlog writeBatch failed:", err)
				continue
			}
			// sync the value log
			if err := db.vlog.sync(); err != nil {
				log.Println("vlog sync failed:", err)
				continue
			}

			// write all keys and positions to index
			if err := db.index.PutBatch(keyPos); err != nil {
				log.Println("index PutBatch failed:", err)
				continue
			}
			if err := db.index.DeleteBatch(deletedKeys); err != nil {
				log.Println("index DeleteBatch failed:", err)
				continue
			}
			// sync the index
			if err := db.index.Sync(); err != nil {
				log.Println("index sync failed:", err)
				continue
			}

			// delete the wal
			if err := table.deleteWAl(); err != nil {
				log.Println("delete wal failed:", err)
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
