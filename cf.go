package lotusdb

import (
	"errors"
	"github.com/flowercorp/lotusdb/memtable"
	"github.com/flowercorp/lotusdb/vlog"
)

var (
	ErrColoumnFamilyNameNil = errors.New("column family name is nil")
)

type ColumnFamily struct {
	activeMem *memtable.Memtable   // Active memtable for writing.
	immuMems  []*memtable.Memtable // Immutable memtables, waiting to be flushed to disk.
	vlog      *vlog.ValueLog
}

func (db *LotusDB) CreateColumnFamily(name string, opts ColumnFamilyOptions) (*ColumnFamily, error) {
	if name == "" {
		return nil, ErrColoumnFamilyNameNil
	}
	// create columm family path.

	// open a memtable.

	// open a wal for memtable(if necessary).

	return nil, nil
}

func (cf *ColumnFamily) Close() error {
	return nil
}

// Put put to default column family.
func (cf *ColumnFamily) Put(key, value []byte) error {
	return nil
}

// Get get from default column family.
func (cf *ColumnFamily) Get(key []byte) error {
	return nil
}

// Delete delete from default column family.
func (cf *ColumnFamily) Delete(key []byte) error {
	return nil
}
