package lotusdb

import (
	"os"
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
	return Options{
		DBPath: path,
		CfOpts: ColumnFamilyOptions{
			CfName:              DefaultColumnFamilyName,
			MemtableSize:        64 << 20,
			MemtableNums:        5,
			MemSpaceWaitTimeout: time.Millisecond * 100,
			FlushBatchSize:      100000,
			WalMMap:             false,
			ValueLogFileSize:    1024 << 20,
			ValueLogMmap:        false,
			ValueLogGCRatio:     0.5,
			ValueLogGCInterval:  time.Minute * 10,
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
		ValueLogGCRatio:     0.5,
		ValueLogGCInterval:  time.Minute * 10,
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
	// Default value is 100ms.
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

	// WalBytesFlush can be used to smooth out write I/Os over time. Users shouldn't rely on it for persistency guarantee.
	// Default value is 0, turned off.
	WalBytesFlush uint32

	// ValueLogDir dir path to store value log file, default value is dir path.
	ValueLogDir string

	// ValueLogFileSize size of a single value log file.
	// Default value is 1GB.
	ValueLogFileSize int64

	// ValueLogMmap similar to WalMMap, default value is false.
	ValueLogMmap bool

	// ValueLogGCRatio if discarded data in value log exceeds this ratio, it can be picked up for compaction(garbage collection)
	// And if there are many files reached the ratio, we will pick the highest one by one.
	// The recommended ratio is 0.5, half of the file can be compacted.
	// Default value is 0.5.
	ValueLogGCRatio float64

	// ValueLogGCInterval a background groutine will check and do gc periodically according to the interval.
	// If you don`t want value log file be compacted, set it a Zero time.
	// Default value is 10 minutes.
	ValueLogGCInterval time.Duration
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

	// ExpiredAt time to live for the specified key, must be a time.Unix value.
	// It will be ignored if it`s not a positive number.
	// Default value is 0.
	ExpiredAt int64
}
