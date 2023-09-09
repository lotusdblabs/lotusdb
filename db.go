package lotusdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/gofrs/flock"
	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

const (
	fileLockName = "FLOCK"
)

// DB is the main structure of the LotusDB database.
// It contains all the information needed to operate the database.
//
// DB is thread-safe, and can be used by multiple goroutines.
// But you can not open multiple DBs with the same directory path at the same time.
// ErrDatabaseIsUsing will be returned if you do so.
//
// LotusDB is the most advanced key-value database written in Go.
// It combines the advantages of LSM tree and B+ tree, read and write are both very fast.
// It is also very memory efficient, and can store billions of key-value pairs in a single machine.
type DB struct {
	activeMem *memtable      // Active memtable for writing.
	immuMems  []*memtable    // Immutable memtables, waiting to be flushed to disk.
	index     Index          // index is multi-partition indexes to store key and chunk position.
	vlog      *valueLog      // vlog is the value log.
	fileLock  *flock.Flock   // fileLock to prevent multiple processes from using the same database directory.
	flushChan chan *memtable // flushChan is used to notify the flush goroutine to flush memtable to disk.
	flushLock sync.Mutex     // flushLock is to prevent flush running while compaction doesn't occur
	mu        sync.RWMutex
	closed    bool
	options   Options
	batchPool sync.Pool // batchPool is a pool of batch, to reduce the cost of memory allocation.
}

// Open a database with the specified options.
// If the database directory does not exist, it will be created automatically.
//
// Multiple processes can not use the same database directory at the same time,
// otherwise it will return ErrDatabaseIsUsing.
//
// It will first open the wal to rebuild the memtable, then open the index and value log.
// Return the DB object if succeeded, otherwise return the error.
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
		indexType:       options.IndexType,
		dirPath:         options.DirPath,
		partitionNum:    options.PartitionNum,
		hashKeyFunction: options.KeyHashFunction,
	})
	if err != nil {
		return nil, err
	}

	// open value log
	vlog, err := openValueLog(valueLogOptions{
		dirPath:           options.DirPath,
		segmentSize:       options.ValueLogFileSize,
		blockCache:        options.BlockCache,
		partitionNum:      uint32(options.PartitionNum),
		hashKeyFunction:   options.KeyHashFunction,
		compactBatchCount: options.CompactBatchCount,
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

	// if there are some immutable memtables when opening the database, flush them to disk
	if len(db.immuMems) > 0 {
		for _, table := range db.immuMems {
			db.flushMemtable(table)
		}
	}

	// start flush memtables goroutine asynchronously,
	// memtables with new coming writes will be flushed to disk if the active memtable is full.
	go db.listenMemtableFlush()

	return db, nil
}

// Close the database, close all data files and release file lock.
// Set the closed flag to true.
// The DB instance cannot be used after closing.
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

// Sync all data files to the underlying storage.
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

// Put a key-value pair into the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Put operation.
func (db *DB) Put(key []byte, value []byte, options *WriteOptions) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db).withPendingWrites()
	if err := batch.Put(key, value); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit(options)
}

// Get the value of the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Get operation.
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

// Delete the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Delete operation.
func (db *DB) Delete(key []byte, options *WriteOptions) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db).withPendingWrites()
	if err := batch.Delete(key); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit(options)
}

// Exist checks if the specified key exists in the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Exist operation.
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
	if options.IndexType == Hash {
		return errors.New("hash index is not supported yet")
	}
	if options.DirPath == "" {
		return errors.New("the database directory path can not be empty")
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

// get all memtables, including active memtable and immutable memtables.
// must be called with db.mu held.
func (db *DB) getMemTables() []*memtable {
	var tables []*memtable
	tables = append(tables, db.activeMem)

	last := len(db.immuMems) - 1
	for i := range db.immuMems {
		tables = append(tables, db.immuMems[last-i])
	}

	return tables
}

// waitMemtableSpace waits for space in the memtable.
// If the active memtable is full, it will be flushed to disk by the background goroutine.
// But if the flush speed is slower than the write speed, there may be no space in the memtable.
// So the write operation will wait for space in the memtable, and the timeout is specified by WaitMemSpaceTimeout.
func (db *DB) waitMemtableSpace() error {
	if !db.activeMem.isFull() {
		return nil
	}

	timer := time.NewTimer(db.options.WaitMemSpaceTimeout)
	defer timer.Stop()
	select {
	case db.flushChan <- db.activeMem:
		db.immuMems = append(db.immuMems, db.activeMem)
		options := db.activeMem.options
		options.tableId++
		// open a new memtable for writing
		table, err := openMemtable(options)
		if err != nil {
			return err
		}
		db.activeMem = table
	case <-timer.C:
		return errors.New("wait memtable space timeout, try again later")
	}

	return nil
}

// flushMemtable flushes the specified memtable to disk.
// Following steps will be done:
// 1. Iterate all records in memtable, divide them into deleted keys and log records.
// 2. Write the log records to value log, get the positions of keys.
// 3. Write all keys and positions to index.
// 4. Delete the deleted keys from index.
// 5. Delete the wal.
func (db *DB) flushMemtable(table *memtable) {
	db.flushLock.Lock()
	sklIter := table.skl.NewIterator()
	var deletedKeys [][]byte
	var logRecords []*ValueLogRecord

	// iterate all records in memtable, divide them into deleted keys and log records
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
		db.flushLock.Unlock()
		return
	}

	// sync the value log
	if err := db.vlog.sync(); err != nil {
		log.Println("vlog sync failed:", err)
		db.flushLock.Unlock()
		return
	}

	// write all keys and positions to index
	if err := db.index.PutBatch(keyPos); err != nil {
		log.Println("index PutBatch failed:", err)
		db.flushLock.Unlock()
		return
	}
	// delete the deleted keys from index
	if err := db.index.DeleteBatch(deletedKeys); err != nil {
		log.Println("index DeleteBatch failed:", err)
		db.flushLock.Unlock()
		return
	}
	// sync the index
	if err := db.index.Sync(); err != nil {
		log.Println("index sync failed:", err)
		db.flushLock.Unlock()
		return
	}

	// delete the wal
	if err := table.deleteWAl(); err != nil {
		log.Println("delete wal failed:", err)
		db.flushLock.Unlock()
		return
	}

	// delete old memtable kept in memory
	db.mu.Lock()
	if len(db.immuMems) == 1 {
		db.immuMems = db.immuMems[:0]
	} else {
		db.immuMems = db.immuMems[1:]
	}
	db.mu.Unlock()

	db.flushLock.Unlock()
}

func (db *DB) listenMemtableFlush() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case table := <-db.flushChan:
			db.flushMemtable(table)
		case <-sig:
			return
		}
	}
}

// Compact will iterate all values in vlog, and write the valid values to a new vlog file.
// Then replace the old vlog file with the new one, and delete the old one.
func (db *DB) Compact() error {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	openVlogFile := func(part int, ext string) *wal.WAL {
		walFile, err := wal.Open(wal.Options{
			DirPath:        db.vlog.options.dirPath,
			SegmentSize:    db.vlog.options.segmentSize,
			SegmentFileExt: fmt.Sprintf(ext, part),
			BlockCache:     db.vlog.options.blockCache,
			Sync:           false, // we will sync manually
			BytesPerSync:   0,     // the same as Sync
		})
		if err != nil {
			_ = walFile.Delete()
			panic(err)
		}
		return walFile
	}

	g, _ := errgroup.WithContext(context.Background())
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			newVlogFile := openVlogFile(part, tempValueLogFileExt)

			validRecords := make([]*ValueLogRecord, 0, db.vlog.options.compactBatchCount)
			reader := db.vlog.walFiles[part].NewReader()
			var count = 0
			// iterate all records in wal, find the valid records
			for {
				count++
				chunk, pos, err := reader.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					_ = newVlogFile.Delete()
					return err
				}

				record := decodeValueLogRecord(chunk)
				keyPos, err := db.index.Get(record.key)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}

				if keyPos == nil {
					continue
				}
				if keyPos.partition == uint32(part) && reflect.DeepEqual(keyPos.position, pos) {
					validRecords = append(validRecords, record)
				}

				if count%db.vlog.options.compactBatchCount == 0 {
					err := db.rewriteValidRecords(newVlogFile, validRecords, part)
					if err != nil {
						_ = newVlogFile.Delete()
						return err
					}
					validRecords = validRecords[:0]
				}
			}

			if len(validRecords) > 0 {
				err := db.rewriteValidRecords(newVlogFile, validRecords, part)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}
			}

			// replace the wal with the new one.
			_ = db.vlog.walFiles[part].Delete()
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				return err
			}
			db.vlog.walFiles[part] = openVlogFile(part, valueLogFileExt)

			return nil
		})
	}

	return g.Wait()
}

func (db *DB) rewriteValidRecords(walFile *wal.WAL, validRecords []*ValueLogRecord, part int) error {
	var positions []*KeyPosition
	for _, record := range validRecords {
		pos, err := walFile.Write(encodeValueLogRecord(record))
		if err != nil {
			return err
		}

		positions = append(positions, &KeyPosition{
			key:       record.key,
			partition: uint32(part),
			position:  pos},
		)
	}
	return db.index.PutBatch(positions)
}
