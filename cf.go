package lotusdb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/flowercorp/lotusdb/index"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/memtable"
	"github.com/flowercorp/lotusdb/util"
	"github.com/flowercorp/lotusdb/vlog"
)

var (
	// ErrColoumnFamilyNil .
	ErrColoumnFamilyNil = errors.New("column family name is nil")

	// ErrColoumnFamilyExists .
	ErrColoumnFamilyExists = errors.New("column family is already exist")

	// ErrWaitMemSpaceTimeout .
	ErrWaitMemSpaceTimeout = errors.New("wait enough memtable space for writing timeout")
)

// ColumnFamily is a namespace of keys and values.
type ColumnFamily struct {
	activeMem *memtable.Memtable   // Active memtable for writing.
	immuMems  []*memtable.Memtable // Immutable memtables, waiting to be flushed to disk.
	vlog      *vlog.ValueLog       // Value Log.
	indexer   index.Indexer
	flushChn  chan *memtable.Memtable
	opts      ColumnFamilyOptions
	mu        sync.Mutex
}

// OpenColumnFamily open a new or existed column family.
func (db *LotusDB) OpenColumnFamily(opts ColumnFamilyOptions) (*ColumnFamily, error) {
	if opts.CfName == "" {
		return nil, ErrColoumnFamilyNil
	}
	// use db path as default column family path.
	if opts.DirPath == "" {
		opts.DirPath = db.opts.DBPath
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.cfs[opts.CfName]; ok {
		return nil, ErrColoumnFamilyExists
	}
	// create columm family path.
	if !util.PathExist(opts.DirPath) {
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	cf := &ColumnFamily{
		opts:     opts,
		flushChn: make(chan *memtable.Memtable, opts.MemtableNums-1),
	}
	// open active and immutable memtables.
	if err := cf.openMemtables(); err != nil {
		return nil, err
	}

	// create bptree indexer.
	bptreeOpt := &index.BPTreeOptions{
		IndexType:        index.BptreeBoltDB,
		ColumnFamilyName: opts.CfName,
		BucketName:       []byte(opts.CfName),
		DirPath:          opts.DirPath,
	}
	indexer, err := index.NewIndexer(bptreeOpt)
	if err != nil {
		return nil, err
	}
	cf.indexer = indexer

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

	db.cfs[opts.CfName] = cf
	go cf.listenAndFlush()
	return cf, nil
}

func (cf *ColumnFamily) Close() error {
	return nil
}

// Put put to current column family.
func (cf *ColumnFamily) Put(key, value []byte) error {
	return cf.PutWithOptions(key, value, nil)
}

// PutWithOptions put to current column family with options.
func (cf *ColumnFamily) PutWithOptions(key, value []byte, opt *WriteOptions) error {
	// waiting for enough memtable sapce to write.
	if err := cf.waitMemSpace(); err != nil {
		return err
	}

	var memOpts memtable.Options
	if opt != nil {
		memOpts.Sync = opt.Sync
		memOpts.DisableWal = opt.DisableWal
		memOpts.ExpiredAt = opt.ExpiredAt
	}
	if err := cf.activeMem.Put(key, value, memOpts); err != nil {
		return err
	}
	return nil
}

// Get get from current column family.
func (cf *ColumnFamily) Get(key []byte) ([]byte, error) {
	tables := cf.getMemtables()
	// get from active and immutable memtables.
	for _, mem := range tables {
		if value := mem.Get(key); value != nil {
			return value, nil
		}
	}

	// get from bptree.
	indexMeta, err := cf.indexer.Get(key)
	if err != nil {
		return nil, err
	} else if len(indexMeta.Value) != 0 {
		return indexMeta.Value, nil
	}

	// get value from value log.
	if indexMeta.Size != 0 {
		ve, err := cf.vlog.Read(indexMeta.Fid, indexMeta.Size, indexMeta.Offset)
		if err != nil {
			return nil, err
		}
		if len(ve.Value) != 0 {
			return ve.Value, nil
		}
	}
	return nil, nil
}

// Delete delete from current column family.
func (cf *ColumnFamily) Delete(key []byte) error {
	return cf.DeleteWithOptions(key, nil)
}

// DeleteWithOptions delete from current column family with options.
func (cf *ColumnFamily) DeleteWithOptions(key []byte, opt *WriteOptions) error {
	var memOpts memtable.Options
	if opt != nil {
		memOpts.Sync = opt.Sync
		memOpts.DisableWal = opt.DisableWal
		memOpts.ExpiredAt = opt.ExpiredAt
	}
	if err := cf.activeMem.Delete(key, memOpts); err != nil {
		return err
	}
	return nil
}

// Stat returns some statistics info of current column family.
func (cf *ColumnFamily) Stat() error {
	return nil
}

func (cf *ColumnFamily) openMemtables() error {
	// read wal dirs.
	fileInfos, err := ioutil.ReadDir(cf.opts.WalDir)
	if err != nil {
		return err
	}

	// find all wal files`id.
	var fids []uint32
	for _, file := range fileInfos {
		if !strings.HasSuffix(file.Name(), logfile.WalSuffixName) {
			continue
		}
		splitNames := strings.Split(file.Name(), ".")
		fid, err := strconv.Atoi(splitNames[0])
		if err != nil {
			return err
		}
		fids = append(fids, uint32(fid))
	}

	// load in descending order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] > fids[j]
	})
	if len(fids) == 0 {
		fids = append(fids, logfile.InitialLogFileId)
	}

	tableType := cf.getMemtableType()
	var ioType = logfile.FileIO
	if cf.opts.WalMMap {
		ioType = logfile.MMap
	}

	memOpts := memtable.Options{
		Path:     cf.opts.WalDir,
		Fsize:    cf.opts.MemtableSize,
		TableTyp: tableType,
		IoType:   ioType,
	}
	for i, fid := range fids {
		memOpts.Fid = fid
		table, err := memtable.OpenMemTable(memOpts)
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

func (cf *ColumnFamily) getMemtables() []*memtable.Memtable {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	var tables = []*memtable.Memtable{cf.activeMem}
	for _, tb := range cf.immuMems {
		tables = append(tables, tb)
	}
	return tables
}
