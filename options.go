package lotusdb

import (
	"os"
	"path/filepath"
	"time"
)

const (
	// DefaultColumnFamilyName default cf name, we will create a default column family when opening a db.
	DefaultColumnFamilyName = "cf_default"
	separator               = string(os.PathSeparator)
	lockFileName            = "FLOCK"
)

// DefaultOptions default options for opening a LotusDB.
func DefaultOptions(path string) Options {
	cfPath, _ := filepath.Abs(filepath.Join(path, DefaultColumnFamilyName))
	return Options{
		DBPath: path,
		CfOpts: ColumnFamilyOptions{
			CfName:              DefaultColumnFamilyName,
			DirPath:             cfPath,
			MemtableSize:        64 << 20,
			MemtableNums:        5,
			MemSpaceWaitTimeout: time.Millisecond * 100,
			IndexerDir:          cfPath,
			FlushBatchSize:      100000,
			WalMMap:             false,
			ValueLogDir:         cfPath,
			ValueLogFileSize:    1024 << 20,
			ValueLogMmap:        false,
			ValueThreshold:      0,
		},
	}
}

// DefaultColumnFamilyOptions default options for opening a column family.
func DefaultColumnFamilyOptions(name string) ColumnFamilyOptions {
	return ColumnFamilyOptions{
		CfName:              name,
		MemtableSize:        64 << 20,
		MemtableNums:        5,
		MemSpaceWaitTimeout: time.Millisecond * 100,
		FlushBatchSize:      100000,
		WalMMap:             false,
		ValueLogFileSize:    1024 << 20,
		ValueLogMmap:        false,
		ValueThreshold:      0,
	}
}

// Options for opening a db.
type Options struct {
	// DBPath db path, will be created automatically if not exist.
	DBPath string

	// CfOpts options for column family.
	CfOpts ColumnFamilyOptions
}

// ColumnFamilyOptions for column family.
type ColumnFamilyOptions struct {

	// CfName column family name, can`t be nil.
	CfName string

	// DirPath dir path for column family, default value is db path+CfName .
	// will be created automatically if not exist.
	DirPath string

	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
	MemtableSize uint32

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default value is 5.
	MemtableNums int

	// MemSpaceWaitTimeout represents timeout for waiting enough memtable space to write.
	// In this scenario will wait: memtable has reached the maximum nums, and has no time to be flushed into disk.
	MemSpaceWaitTimeout time.Duration

	// IndexerDir dir path to store index meta data, default value is dir path.
	IndexerDir string

	// FlushBatchSize Since we use b+tree to store index info on disk right now.
	// We need flush data to b+tree in batches, which can minimize random IO and reduce write amplification.
	// Default value is 100000.
	FlushBatchSize int

	// WalMMap represents whether to use memory map for reading and writing on WAL.
	// Setting false means to use standard file io.
	// Default value is false.
	WalMMap bool

	// ValueLogDir dir path to store value log file, default value is dir path.
	ValueLogDir string

	// ValueLogFileSize size of a single value log file.
	// Default value is 1GB.
	ValueLogFileSize int64

	// ValueLogMmap similar to WalMMap, default value is false.
	ValueLogMmap bool

	// ValueThreshold sets the threshold used to decide whether a value is stored directly in the Index(B+Tree right now) or separately in value log file.
	// If the threshold is zero, that means all values will be stored in value log file.
	// Default value is 0.
	ValueThreshold int
}

// WriteOptions set optional params for PutWithOptions and DeleteWithOptions.
// If use Put and Delete (without options), that means to use the default values.
type WriteOptions struct {
	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.

	// Default value is false.
	Sync bool

	// DisableWal if true, writes will not first go to the write ahead log, and the write may get lost after a crash.
	// Setting true only if don`t care about the data loss.
	// Default value is false.
	DisableWal bool

	// ExpiredAt time to live for the specified key, a time.Unix value.
	// It will be ignored if it`s not a positive number.
	// Default value is 0.
	ExpiredAt int64
}
