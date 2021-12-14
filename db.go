package lotusdb

import (
	"github.com/flowercorp/lotusdb/memtable"
)

type LotusDB struct {
	activeMem *memtable.Memtable   // Active memtable for writing.
	immuMems  []*memtable.Memtable // Immutable memtables, waiting to be flushed to disk.
}
