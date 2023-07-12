package lotusdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
)

const (
	fileLockName = "FLOCK"
)

type DB struct {
	// Active memtable for writing.
	activeMem *memtable
	// Immutable memtables, waiting to be flushed to disk.
	immuMems []*memtable
	// index is multi-partition bptree to store key and chunk position.
	index Index
	// vlog is the value log.
	vlog *valueLog
	// fileLock to prevent multiple processes from using the same database directory.
	fileLock *flock.Flock
	mu       sync.RWMutex
	closed   bool
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
		dirPath:      options.DirPath,
		segmentSize:  options.ValueLogFileSize,
		blockCache:   options.BlockCache,
		partitionNum: options.PartitionNum,
	})
	if err != nil {
		return nil, err
	}

	return &DB{
		activeMem: memtables[len(memtables)-1],
		immuMems:  memtables[:len(memtables)-1],
		index:     index,
		vlog:      vlog,
		fileLock:  fileLock,
	}, nil
}

func (db *DB) Close() error {
	// close all memtables

	// close index

	// close value log

	// release file lock

	return nil
}

func (db *DB) Sync() error {
	// sync all wal of memtables

	// sync index

	// sync value log
	return nil
}

func (db *DB) Stat() *Stat {
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	// call batch put
	return nil
}

func (db *DB) Get(key []byte, value []byte) ([]byte, error) {
	// call batch get
	return nil, nil
}

func (db *DB) Delete(key []byte) error {
	// call batch delete
	return nil
}

func (db *DB) Exist(key []byte) (bool, error) {
	// call batch exist
	return false, nil
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
