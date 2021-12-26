package lotusdb

type Options struct {
	// MemtableSize
	MemtableSize uint64

	MemtableNum int

	WalDir string

	WalMMap bool

	DisableWal bool
}

type ColumnFamilyOptions struct {
	ColumnFamilyDir string
}
