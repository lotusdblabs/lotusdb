package lotusdb

import (
	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/rosedblabs/wal"
)

// memtable is an in-memory data structure holding data before they are flushed into index and value log.
// Currently, the only supported data structure is skip list, see arenaskl.Skiplist.
//
// New writes always insert data to memtable, and reads has query from memtable
// before reading from indexer and vlog, because memtable`s data is newer.
// Once a memtable is full(memtable has its threshold, see MemtableSize in options),
// it becomes immutable and replaced by a new memtable.
// A background goroutine will flush the content of memtable into index and vlog,
// after that the memtable can be deleted.
type memtable struct {
	wal *wal.WAL
	skl *arenaskl.Skiplist
}

// memtable holds a wal(write ahead log), so when opening a memtable,
// actually it open the corresponding wal file.
// and load all entries from wal to rebuild the content of the skip list.
func openMemtable() (*memtable, error) {
	return nil, nil
}

// put inserts a key-value pair into memtable.
func (mt *memtable) put(key []byte, value []byte) error {
	return nil
}

// get value from memtable.
func (mt *memtable) get(key []byte) (bool, []byte) {
	return false, nil
}

// delete operation is to put a key and a special tombstone value.
func (mt *memtable) delete(key []byte) error {
	return mt.put(key, nil)
}
