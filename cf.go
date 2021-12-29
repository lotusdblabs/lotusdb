package lotusdb

import (
	"errors"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/memtable"
	"github.com/flowercorp/lotusdb/util"
	"github.com/flowercorp/lotusdb/vlog"
	"os"
)

var (
	ErrColoumnFamilyNil = errors.New("column family name is nil")
)

type ColumnFamily struct {
	activeMem *memtable.Memtable   // Active memtable for writing.
	immuMems  []*memtable.Memtable // Immutable memtables, waiting to be flushed to disk.
	vlog      *vlog.ValueLog
	opts      ColumnFamilyOptions
}

// OpenColumnFamily open a new or existed column family.
func (db *LotusDB) OpenColumnFamily(opts ColumnFamilyOptions) (*ColumnFamily, error) {
	if opts.CfName == "" {
		return nil, ErrColoumnFamilyNil
	}

	// create columm family path.
	if !util.PathExist(opts.DirPath) {
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	cf := &ColumnFamily{opts: opts}
	// open all immutable memtablse.
	if err := cf.openMemtables(); err != nil {
		return nil, err
	}

	// open value log.
	var ioType = logfile.FileIO
	if opts.ValueLogMmap {
		ioType = logfile.MMap
	}
	valueLog, err := vlog.OpenValueLog(opts.ValueLogDir, opts.ValueLogBlockSize, ioType)
	if err != nil {
		return nil, err
	}
	cf.vlog = valueLog

	return cf, nil
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

func (cf *ColumnFamily) openMemtables() error {

	return nil
}
