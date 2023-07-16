package lotusdb

import (
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/rosedblabs/wal"
)

const (
	fileLockName = "FLOCK"
)

type DB struct {
	activeMem *memtable    // Active memtable for writing.
	immuMems  []*memtable  // Immutable memtables, waiting to be flushed to disk.
	memNums   int          // MemtableNums represents maximum number of memtables to keep in memory before flushing
	index     Index        // index is multi-partition bptree to store key and chunk position.
	vlog      *valueLog    // vlog is the value log.
	fileLock  *flock.Flock // fileLock to prevent multiple processes from using the same database directory.
	flushChan chan *memtable
	mu        sync.RWMutex
	closed    bool
}

type partPosition struct {
	partIndex   uint32
	walPosition *wal.ChunkPosition
}

type Stat struct {
}

func Open(options Options) (*DB, error) {
	// check whether all options are valid
	if err := validateOptions(&options); err != nil {
		return nil, err
	}

	// create the directory if not exist
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
		indexType:    indexBoltDB,
		dirPath:      options.DirPath,
		partitionNum: options.PartitionNum,
	})
	if err != nil {
		return nil, err
	}

	// open value log
	vlog, err := openValueLog(valueLogOptions{
		dirPath:     options.DirPath,
		segmentSize: options.ValueLogFileSize,
		blockCache:  options.BlockCache,
		numPartions: uint32(options.PartitionNum),
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
		batch.Commit(nil)
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
		batch.Commit(nil)
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
			indexRecords := []*IndexRecord{}
			logRecords := []*ValueLogRecord{}
			for sklIter.SeekToFirst(); sklIter.Valid(); sklIter.Next() {
				key, valueStruct := sklIter.Key(), sklIter.Value()
				if valueStruct.Meta == LogRecordDeleted {
					deletedKeys = append(deletedKeys, key)
				} else {
					logRecord := ValueLogRecord{key: key, value: valueStruct.Value}
					logRecords = append(logRecords, &logRecord)
				}
			}
			_ = sklIter.Close()

			// flush to vlog
			keyPos, err := db.vlog.writeBatch(logRecords)
			if err != nil {
				panic("vlog writeBatch failed!\n")
			}

			// flush to index
			for i := 0; i < len(keyPos); i++ {
				indexRec := IndexRecord{key: keyPos[i].key, position: keyPos[i].pos}
				indexRecords = append(indexRecords, &indexRec)
			}
			if err := db.index.PutBatch(indexRecords); err != nil {
				panic("index PutBatch failed!\n")
			}
			if err := db.index.DeleteBatch(deletedKeys); err != nil {
				panic("index DeleteBatch failed!\n")
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
