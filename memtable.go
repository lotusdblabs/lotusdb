package lotusdb

import (
	"errors"
	"fmt"
	"io"
	"sync"

	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/lotusdblabs/lotusdb/v2/logger"
	"github.com/rosedblabs/wal"
)

const paddedSize = 64

var ErrValueTooBig = errors.New("value is too big to fit into memtable")

// memtable is an in-memory data structure holding data before they are flushed into index and value log.
// Currently the only supported data structure is skip list, see arenaskl.Skiplist.
//
// New writes always insert data to memtable, and reads has query from memtable
// before reading from indexer and vlog, because memtable`s data is newer.
// Once a memtable is full(memtable has its threshold, see MemtableSize in options),
// it becomes immutable and replaced by a new memtable.
// A background goroutine will flush the content of memtable into index and vlog,
// after that the memtable can be deleted.
type (
	memtable struct {
		sync.RWMutex
		wal     *wal.WAL
		skl     *arenaskl.Skiplist
		sklIter *arenaskl.Iterator
		opts    *memOptions
	}

	memOptions struct {
		path         string
		walId        uint32
		memSize      uint32
		walByteFlush uint32
		WALSync      bool // WAL flush immediately after each writing
	}

	memValue struct {
		value []byte
		typ   byte
	}
)

// memtable holds a wal(write ahead log), so when opening a memtable,
// actually it open the corresponding wal file.
// and load all entries from wal to rebuild the content of the skip list.
func openMemtable(opts *memOptions) (*memtable, error) {
	// init skip list and arena.
	skl := arenaskl.NewSkiplist(int64(opts.memSize) + int64(arenaskl.MaxNodeSize))
	sklIter := skl.NewIterator()
	table := &memtable{opts: opts, skl: skl, sklIter: sklIter}

	// init wal and wal options
	wal.DefaultOptions.DirPath = opts.path
	wal.DefaultOptions.SementFileExt = ".SEG" + "." + fmt.Sprint(opts.walId)
	wal.DefaultOptions.Sync = opts.WALSync
	if opts.walByteFlush > 0 {
		wal.DefaultOptions.BytesPerSync = opts.walByteFlush
	}

	wal, err := wal.Open(wal.DefaultOptions)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	// load all entries from wal to rebuild the content of the skip list
	reader := wal.NewReader()
	for {
		entryBuf, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Errorf("put value into skip list err.%+v", err)
			return nil, err
		}
		entry := decodeLogRecord(entryBuf)
		if err != nil {
			logger.Errorf("decode err.%+v", err)
		}
		mv := &memValue{
			value: entry.Value,
			typ:   byte(entry.Type),
		}
		mvBuf := mv.encode()
		var errSeek error
		table.Lock()
		table.skl.Put(y.KeyWithTs(entry.Key, 0), y.ValueStruct{Value: mvBuf})
		table.Unlock()
		if errSeek != nil {
			logger.Errorf("put value into skip list err.%+v", err)
			return nil, errSeek
		}
	}
	return table, nil
}

// put inserts a key-value pair into memtable.
func (mt *memtable) put(key []byte, value []byte, deleted bool, opts EntryOptions) error {
	// new an entry
	entry := &LogRecord{Key: key, Value: value}
	if deleted {
		entry.Type = LogRecordDeleted
	}
	buf := encodeLogRecord(entry)

	// write entry into wal first.
	if !opts.DisableWal && mt.wal != nil {
		if _, err := mt.wal.Write(buf); err != nil {
			return err
		}
		// if Sync is ture in WriteOptions, syncWal will be true.
		// otherwise, wal will Sync automatically throughput BytesPerSync parameter
		if opts.Sync && !mt.opts.WALSync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}

	// write data into skip list in memory.
	mt.Lock()
	defer mt.Unlock()
	mv := memValue{value: value, typ: byte(entry.Type)}
	mvBuf := mv.encode()
	mt.skl.Put(y.KeyWithTs(key, 0), y.ValueStruct{Value: mvBuf})

	return nil
}

// get value from memtable
// if the specified key is marked as deleted or expired, a true bool value is returned.
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.RLock()
	defer mt.RUnlock()

	valStru := mt.skl.Get(y.KeyWithTs(key, 0))

	if valStru.Value == nil {
		return false, nil
	}
	mv := decodeMemValue(valStru.Value)
	// ignore deleted key.
	if mv.typ == byte(LogRecordDeleted) {
		return true, nil
	}
	return false, mv.value
}

// delete operation is to put a key and a special tombstone value.
func (mt *memtable) delete(key []byte, opts EntryOptions) error {
	return mt.put(key, nil, true, opts)
}

func (mv *memValue) encode() []byte {
	buf := make([]byte, len(mv.value)+1)
	buf[0] = mv.typ
	copy(buf[1:], mv.value)
	return buf
}

func decodeMemValue(buf []byte) memValue {
	var index = 1
	return memValue{typ: buf[0], value: buf[index:]}
}
