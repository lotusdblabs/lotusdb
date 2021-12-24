package lotusdb

import (
	"github.com/flowercorp/lotusdb/memtable"
)

type LotusDB struct {
	activeMem *memtable.Memtable   // Active memtable for writing.
	immuMems  []*memtable.Memtable // Immutable memtables, waiting to be flushed to disk.
	lockMgr   *LockMgr             // global lock manager that guarantees consistency of read and write.
}

func Open() (*LotusDB, error) {
	// recover data from wal, build memtable.
	return nil, nil
}

func (db *LotusDB) Close() error {
	return nil
}
