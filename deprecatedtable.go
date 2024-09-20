package lotusdb

import (
	"github.com/google/uuid"
)

type ThresholdState int

const (
	ArriveLowerThreshold int = iota
	ArriveUpperThreshold
)

type (
	// Deprecatedtable is used to store old information about deleted/updated keys.
	// for every write/update generated an uuid, we store uuid in the table.
	// It is useful in compaction, allowing us to know whether the kv
	// in the value log is up-to-date without accessing the index.
	// we always build deprecatedtable immediately after compaction.
	deprecatedtable struct {
		partition int
		// table   map[uint32]map[uuid.UUID]bool // we store deprecated uuid of keys,in memory
		table map[uuid.UUID]bool
		size  uint32
	}

	// used to send message to autoCompact.
	deprecatedState struct {
		thresholdState ThresholdState
	}
)

// Create a new deprecatedtable.
func newDeprecatedTable(partition int) *deprecatedtable {
	return &deprecatedtable{
		partition: partition,
		table:     make(map[uuid.UUID]bool),
		size:      0,
	}
}

// Add a uuid to the specified key.
func (dt *deprecatedtable) addEntry(id uuid.UUID) {
	dt.table[id] = true
	dt.size++
}

func (dt *deprecatedtable) existEntry(id uuid.UUID) bool {
	return dt.table[id]
}

func (dt *deprecatedtable) clean() {
	dt.table = make(map[uuid.UUID]bool)
	dt.size = 0
}
