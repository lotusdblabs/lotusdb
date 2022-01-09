package lotusdb

import (
	"os"
	"time"
)

const (
	DefaultColumnFamilyName = "cf_default"
	separator               = string(os.PathSeparator)
	lockFileName            = "FLOCK"
)

type MemTableType int8

const (
	SkipList MemTableType = iota
	HashSkipList
)

func DefaultOptions(path string) Options {
	cfPath := path + separator + DefaultColumnFamilyName
	return Options{
		DBPath: path,
		CfOpts: ColumnFamilyOptions{
			CfName:              DefaultColumnFamilyName,
			DirPath:             cfPath,
			MemtableSize:        64 << 20,
			MemtableNums:        5,
			MemtableType:        SkipList,
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

func DefaultColumnFamilyOptions(name string) ColumnFamilyOptions {
	return ColumnFamilyOptions{
		CfName:              name,
		MemtableSize:        64 << 20, // 64MB
		MemtableNums:        5,
		MemtableType:        SkipList,
		MemSpaceWaitTimeout: time.Millisecond * 100,
		FlushBatchSize:      100000,
		WalMMap:             false,
		ValueLogBlockSize:   1024 << 20, // 1GB
		ValueLogMmap:        false,
		ValueThreshold:      0,
	}
}

// Options for db.
type Options struct {
	DBPath string
	CfOpts ColumnFamilyOptions
}

// ColumnFamilyOptions for column family.
type ColumnFamilyOptions struct {

	// CfName
	CfName string

	// DirPath
	DirPath string

	// MemtableSize
	MemtableSize int64

	// MemtableNums max numbers of memtable
	MemtableNums int

	MemtableType MemTableType

	MemSpaceWaitTimeout time.Duration

	IndexerDir string

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
