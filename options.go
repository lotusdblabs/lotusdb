package lotusdb

import "os"

const (
	defaultColumnFamilyName = "cf_default"
	pathSeparator           = string(os.PathSeparator)
)

const (
	DefaultDBPath = "/tmp/lotusdb"
)

const (
	DefaultVLogBlockSize = 256 * 1024 * 1024 // 256MB
)

type MemTableType int8

const (
	SkipList MemTableType = iota
	HashSkipList
)

// Options for db.
type Options struct {
	DBPath string
	CfOpts ColumnFamilyOptions
}

// ColumnFamilyOptions for column family.
type ColumnFamilyOptions struct {
	CfName string
	// DirPath
	DirPath string

	FileSize int64

	// MemtableSize
	MemtableSize uint64

	// the number of memtable
	MemtableNum int

	MemtableType MemTableType

	WalDir string

	WalMMap bool

	DisableWal bool

	ValueLogDir string

	ValueLogBlockSize int64

	ValueLogMmap bool
}
