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

// Options for db.
type Options struct {
	DBPath string
}

// ColumnFamilyOptions for column family.
type ColumnFamilyOptions struct {
	Name string
	// DirPath
	DirPath string

	// MemtableSize
	MemtableSize uint64

	// the number of memtable
	MemtableNum int

	MentableAlgorithm int

	WalDir string

	WalMMap bool

	DisableWal bool

	ValueLogDir string

	ValueLogBlockSize int64

	ValueLogMmap bool
}
