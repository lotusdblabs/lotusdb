package lotusdb

import (
	"errors"
	"fmt"
	"github.com/flower-corp/lotusdb/flock"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/flower-corp/lotusdb/index"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/util"
	"github.com/flower-corp/lotusdb/vlog"
)

var (
	// ErrColoumnFamilyNil column family name is nil.
	ErrColoumnFamilyNil = errors.New("column family name is nil")

	// ErrWaitMemSpaceTimeout wait enough memtable space for writing timeout.
	ErrWaitMemSpaceTimeout = errors.New("wait enough memtable space for writing timeout, retry later")
)

// ColumnFamily is a namespace of keys and values.
// Each key-value pair in LotusDB is associated with exactly one Column Family.
// If there is no Column Family specified, key-value pair is associated with Column Family "cf_default".
// Column Families provide a way to logically partition the database.
type ColumnFamily struct {
	// Active memtable for writing.
	activeMem *memtable
	// Immutable memtables, waiting to be flushed to disk.
	immuMems []*memtable
	// Value Log(Put value into value log according to options ValueThreshold).
	vlog *vlog.ValueLog
	// Store keys and meta info.
	indexer index.Indexer
	// When the active memtable is full, send it to the flushChn, see listenAndFlush.
	flushChn chan *memtable
	opts     ColumnFamilyOptions
	mu       sync.RWMutex
	// Prevent concurrent db using.
	// At least one FileLockGuard(cf/indexer/vlog dirs are all the same).
	// And at most three FileLockGuards(cf/indexer/vlog dirs are all different).
	dirLocks []*flock.FileLockGuard
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
	if opts.IndexerDir == "" {
		opts.IndexerDir = opts.DirPath
	}
	if opts.ValueLogDir == "" {
		opts.ValueLogDir = opts.DirPath
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if columnFamily, ok := db.cfs[opts.CfName]; ok {
		return columnFamily, nil
	}
	// create dir paths.
	paths := []string{opts.DirPath, opts.IndexerDir, opts.ValueLogDir}
	for _, path := range paths {
		if !util.PathExist(path) {
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				return nil, err
			}
		}
	}

	// acquire file lock to lock cf/indexer/vlog directory.
	flocks, err := acquireDirLocks(opts.DirPath, opts.IndexerDir, opts.ValueLogDir)
	if err != nil {
		return nil, fmt.Errorf("another process is using dir.%v", err.Error())
	}

	cf := &ColumnFamily{
		opts:     opts,
		dirLocks: flocks,
		flushChn: make(chan *memtable, opts.MemtableNums-1),
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
		DirPath:          opts.IndexerDir,
		BatchSize:        opts.FlushBatchSize,
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
	valueLog, err := vlog.OpenValueLog(opts.ValueLogDir, opts.ValueLogFileSize, ioType)
	if err != nil {
		return nil, err
	}
	cf.vlog = valueLog

	db.cfs[opts.CfName] = cf
	go cf.listenAndFlush()
	return cf, nil
}

// Close close current colun family.
func (cf *ColumnFamily) Close() error {
	for _, dirLock := range cf.dirLocks {
		dirLock.Release()
	}
	return nil
}

// Put put to current column family.
func (cf *ColumnFamily) Put(key, value []byte) error {
	return cf.PutWithOptions(key, value, nil)
}

// PutWithOptions put to current column family with options.
func (cf *ColumnFamily) PutWithOptions(key, value []byte, opt *WriteOptions) error {
	// waiting for enough memtable sapce to write.
	size := uint32(len(key) + len(value))
	if err := cf.waitMemSpace(size); err != nil {
		return err
	}
	if opt == nil {
		opt = new(WriteOptions)
	}
	if err := cf.activeMem.put(key, value, false, *opt); err != nil {
		return err
	}
	return nil
}

// Get get value by the specified key from current column family.
func (cf *ColumnFamily) Get(key []byte) ([]byte, error) {
	tables := cf.getMemtables()
	// get from active and immutable memtables.
	for _, mem := range tables {
		if value := mem.get(key); len(value) != 0 {
			return value, nil
		}
	}

	// get index from bptree.
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
	if opt == nil {
		opt = new(WriteOptions)
	}
	if err := cf.activeMem.delete(key, *opt); err != nil {
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
	fileInfos, err := ioutil.ReadDir(cf.opts.DirPath)
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

	// load memtables in order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	if len(fids) == 0 {
		fids = append(fids, logfile.InitialLogFileId)
	}

	var ioType = logfile.FileIO
	if cf.opts.WalMMap {
		ioType = logfile.MMap
	}
	memOpts := memOptions{
		path:    cf.opts.DirPath,
		fsize:   int64(cf.opts.MemtableSize),
		ioType:  ioType,
		memSize: cf.opts.MemtableSize,
	}
	for i, fid := range fids {
		memOpts.fid = fid
		table, err := openMemtable(memOpts)
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

func (cf *ColumnFamily) getMemtables() []*memtable {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	immuLen := len(cf.immuMems)
	var tables = make([]*memtable, immuLen+1)
	tables[0] = cf.activeMem
	for idx := 0; idx < immuLen; idx++ {
		tables[idx+1] = cf.immuMems[immuLen-idx-1]
	}
	return tables
}

func acquireDirLocks(cfDir, indexerDir, vlogDir string) ([]*flock.FileLockGuard, error) {
	var dirs = []string{cfDir}
	if indexerDir != cfDir {
		dirs = append(dirs, indexerDir)
	}
	if vlogDir != cfDir && vlogDir != indexerDir {
		dirs = append(dirs, vlogDir)
	}

	var flocks []*flock.FileLockGuard
	for _, dir := range dirs {
		lock, err := flock.AcquireFileLock(dir+separator+lockFileName, false)
		if err != nil {
			return nil, err
		}
		flocks = append(flocks, lock)
	}
	return flocks, nil
}
