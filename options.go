package lotusdb

type Options struct {
	// MemtableSize
	MemtableSize uint64

	MemtableNum int

	WalDir string

	WalMMap bool

	ColumnFamilyDir string
}
