package lotusdb

import "os"

const (
	DefaultColumnFamilyName = "cf_default"
	separator               = string(os.PathSeparator)
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
			CfName:            DefaultColumnFamilyName,
			DirPath:           cfPath,
			MemtableSize:      64 << 20,
			MemtableNums:      5,
			MemtableType:      SkipList,
			WalDir:            cfPath,
			WalMMap:           false,
			DisableWal:        false,
			ValueLogDir:       cfPath,
			ValueLogBlockSize: 1024 << 20,
			ValueLogMmap:      false,
		},
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

	WalDir string

	WalMMap bool

	DisableWal bool

	ValueLogDir string

	ValueLogBlockSize int64

	ValueLogMmap bool
}
