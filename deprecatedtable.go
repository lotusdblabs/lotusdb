package lotusdb

import (
	"github.com/google/uuid"
)

type ThresholdState int

const (
	ArriveAdvisedThreshold int = iota // Recommended to perform a compaction at this time
	ArriveForceThreshold              // At this point, force a compaction
	UnarriveThreshold                 // Not require compaction
)

type (
	// Deprecatedtable is used to store old information about deleted/updated keys.
	// for every write/update generated an uuid, we store uuid in the table.
	// It is useful in compaction, allowing us to know whether the kv
	// in the value log is up-to-date without accessing the index.
	deprecatedtable struct {
		partition int                    // which shard in vlog
		table     map[uuid.UUID]struct{} // we store deprecated uuid of keys,in memory
		size      uint32                 // number of deprecated entry now
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
		table:     make(map[uuid.UUID]struct{}),
		size:      0,
	}
}

// Add a uuid to the specified key.
func (dt *deprecatedtable) addEntry(id uuid.UUID) {
	dt.table[id] = struct{}{}
	dt.size++
}

func (dt *deprecatedtable) existEntry(id uuid.UUID) bool {
	_, exists := dt.table[id]
	return exists
}

func (dt *deprecatedtable) clean() {
	dt.table = make(map[uuid.UUID]struct{})
	dt.size = 0
}
