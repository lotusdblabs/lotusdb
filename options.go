package lotusdb

const (
	DefaultDBPath = "/tmp/lotusdb"
)

const (
	DefaultVLogBlockSize = 16 * 1024 * 1024 // 16MB
)

// Options for db.
type Options struct {
	DBPath string
}

// ColumnFamilyOptions for column family.
type ColumnFamilyOptions struct {
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

	ValueLogBlockSize int64
}
