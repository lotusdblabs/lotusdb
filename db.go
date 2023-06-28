package lotusdb

type DB struct {
	// Active memtable for writing.
	activeMem *memtable
	// Immutable memtables, waiting to be flushed to disk.
	immuMems []*memtable
	// vlog is the value log.
	vlog *valueLog
}
