package lotusdb

import (
	"errors"
	"fmt"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/memtable"
	"github.com/flowercorp/lotusdb/util"
	"github.com/flowercorp/lotusdb/vlog"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	ErrColoumnFamilyNil = errors.New("column family name is nil")
)

type ColumnFamily struct {
	activeMem *memtable.Memtable   // Active memtable for writing.
	immuMems  []*memtable.Memtable // Immutable memtables, waiting to be flushed to disk.
	vlog      *vlog.ValueLog       // Value Log.
	opts      ColumnFamilyOptions
}

// OpenColumnFamily open a new or existed column family.
func (db *LotusDB) OpenColumnFamily(opts ColumnFamilyOptions) (*ColumnFamily, error) {
	if opts.CfName == "" {
		return nil, ErrColoumnFamilyNil
	}
	// use db path.
	if opts.DirPath == "" {
		opts.DirPath = db.opts.DBPath
	}

	// create columm family path.
	if !util.PathExist(opts.DirPath) {
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	cf := &ColumnFamily{opts: opts}
	// open active and immutable memtables.
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
	// read wal dirs.
	fileInfos, err := ioutil.ReadDir(cf.opts.WalDir)
	if err != nil {
		return err
	}

	var fids []uint32
	for _, file := range fileInfos {
		if strings.HasSuffix(file.Name(), logfile.WalSuffixName) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return err
			}
			fids = append(fids, uint32(fid))
		}
	}

	// load in descsending order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] > fids[j]
	})

	tableType := cf.getMemtableType()
	var ioType = logfile.FileIO
	if cf.opts.WalMMap {
		ioType = logfile.MMap
	}

	if len(fids) == 0 {
		fids = append(fids, 0)
	}
	for i, fid := range fids {
		table, err := memtable.OpenMemTable(cf.opts.WalDir, fid, tableType, ioType)
		if err != nil {
			return err
		}
		if i == 0 {
			cf.activeMem = table
		} else {
			cf.immuMems = append(cf.immuMems, table)
		}
	}
	return nil
}

func (cf *ColumnFamily) getMemtableType() memtable.TableType {
	switch cf.opts.MemtableType {
	case SkipList:
		return memtable.SkipListRep
	case HashSkipList:
		return memtable.HashSkipListRep
	default:
		panic(fmt.Sprintf("unsupported memtable type: %d", cf.opts.MemtableType))
	}
}
