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
			ValueLogBlockSize:   1024 << 20,
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
		ValueLogBlockSize:   1024 << 20,
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
	// Default MemtableSize is 64MB.
	MemtableSize uint32

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default MemtableNums is 5.
	MemtableNums int

	// MemSpaceWaitTimeout represents timeout for waiting enough memtable space to write.
	// In this scenario will wait: memtable has reached the maximum nums, and has no time to be flushed into disk.
	MemSpaceWaitTimeout time.Duration

	// IndexerDir dir path to store index, default value is dir path.
	IndexerDir string

	// FlushBatchSize since we use b+tree to store index info in disk.
	FlushBatchSize int

	WalMMap bool

	ValueLogDir string

	ValueLogBlockSize int64

	ValueLogMmap bool

	ValueThreshold int
}

// WriteOptions options for writing key and value.
type WriteOptions struct {
	// Sync .
	Sync bool

	// DisableWal .
	DisableWal bool

	// ExpiredAt time to live for the key. time.Unix
	ExpiredAt int64
}
