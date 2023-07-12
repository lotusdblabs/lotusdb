package lotusdb

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// todo in memtable
// 1. put with multiple key/value pairs, guarantee atomicity just like rosedb
// 2. handle the uncommitted records in wal while reopening the memtable

const (
	// the wal file name format is .SEG.%d
	// %d is the unique id of the memtable, used to generate wal file name
	// for example, the wal file name of memtable with id 1 is .SEG.1
	walFileExt     = ".SEG.%d"
	initialTableID = 1
)

type (
	// memtable is an in-memory data structure holding data before they are flushed into index and value log.
	// Currently, the only supported data structure is skip list, see github.com/dgraph-io/badger/v4/skl.
	//
	// New writes always insert data to memtable, and reads has query from memtable
	// before reading from index and value log, because memtable`s data is newer.
	//
	// Once a memtable is full(memtable has its threshold, see MemtableSize in options),
	// it becomes immutable and replaced by a new memtable.
	//
	// A background goroutine will flush the content of memtable into index and vlog,
	// after that the memtable can be deleted.
	memtable struct {
		mu      sync.RWMutex
		wal     *wal.WAL           // write ahead log for the memtable
		skl     *arenaskl.Skiplist // in-memory skip list
		options memtableOptions
	}

	// memtableOptions represents the configuration options for a memtable.
	memtableOptions struct {
		dirPath         string // where write ahead log wal file is stored
		tableId         uint32 // unique id of the memtable, used to generate wal file name
		memSize         uint32 // max size of the memtable
		maxBatchSize    int64  // max entries size of a single batch
		walBytesPerSync uint32 // flush wal file to disk throughput BytesPerSync parameter
		walSync         bool   // WAL flush immediately after each writing
		walBlockCache   uint32 // block cache size of wal
	}
)

func openAllMemtables(options Options) ([]*memtable, error) {
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// get all memtable ids
	var tableIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), walFileExt, &id)
		if err != nil {
			continue
		}
		tableIDs = append(tableIDs, id)
	}

	if len(tableIDs) == 0 {
		tableIDs = append(tableIDs, initialTableID)
	}

	sort.Ints(tableIDs)
	tables := make([]*memtable, len(tableIDs))
	for i, table := range tableIDs {
		table, err := openMemtable(memtableOptions{
			dirPath:         options.DirPath,
			tableId:         uint32(table),
			memSize:         options.MemtableSize,
			maxBatchSize:    0, // todo
			walSync:         options.Sync,
			walBytesPerSync: options.BytesPerSync,
			walBlockCache:   options.BlockCache,
		})
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}

	return tables, nil
}

// memtable holds a wal(write ahead log), so when opening a memtable,
// actually it open the corresponding wal file.
// and load all entries from wal to rebuild the content of the skip list.
func openMemtable(options memtableOptions) (*memtable, error) {
	// init skip list
	skl := arenaskl.NewSkiplist(int64(options.memSize) + options.maxBatchSize)
	table := &memtable{options: options, skl: skl}

	// open the corresponding Write Ahead Log file
	walFile, err := wal.Open(wal.Options{
		DirPath:        options.dirPath,
		SegmentSize:    math.MaxInt, // no limit, guarantee that a wal file only contains one segment file
		SegmentFileExt: fmt.Sprintf(walFileExt, options.tableId),
		BlockCache:     options.walBlockCache,
		Sync:           options.walSync,
		BytesPerSync:   options.walBytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	table.wal = walFile

	// now we get the opened wal file, we need to load all entries
	// from wal to rebuild the content of the skip list
	reader := table.wal.NewReader()
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		record := decodeLogRecord(chunk)
		// handle the batch id todo
		table.skl.Put(y.KeyWithTs(record.Key, 0), y.ValueStruct{Value: record.Value, Meta: record.Type})
	}

	// open and read wal file successfully, return the memtable
	return table, nil
}

// put a key-value pair into memtable
func (mt *memtable) put(key []byte, value []byte, deleted bool, options WriteOptions) error {
	record := &LogRecord{Key: key, Value: value}
	if deleted {
		record.Type = LogRecordDeleted
	}
	encRecord := encodeLogRecord(record)

	// write record into wal first.
	if !options.DisableWal && mt.wal != nil {
		if _, err := mt.wal.Write(encRecord); err != nil {
			return err
		}
		// sync wal file to disk if needed.
		if options.Sync && !mt.options.walSync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}

	// write data into skip list in memory.
	mt.mu.Lock()
	mt.skl.Put(y.KeyWithTs(key, 0), y.ValueStruct{Value: value, Meta: record.Type})
	mt.mu.Unlock()

	return nil
}

// get value from memtable
// if the specified key is marked as deleted, a true bool value is returned.
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	var deleted bool
	valueStruct := mt.skl.Get(y.KeyWithTs(key, 0))
	if valueStruct.Meta == LogRecordDeleted {
		return true, nil
	}

	deleted = valueStruct.Meta == LogRecordDeleted
	return deleted, valueStruct.Value
}

// delete operation is to put a key and a special tombstone value.
func (mt *memtable) delete(key []byte, options WriteOptions) error {
	return mt.put(key, nil, true, options)
}
