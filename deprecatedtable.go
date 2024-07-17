package lotusdb

import (
	"errors"
	"hash"
	"hash/fnv"
	"sync"

	"github.com/google/uuid"
)

type ThresholdState int

const (
	ArriveLowerThreshold int = iota
	ArriveUpperThreshold
)

const (
	KeyHash = 11027
)

var (
	ErrDtFull = errors.New("deprecatedtable full")
)

type (
	// Deprecatedtable is used to store old information about deleted/updated keys.
	// for every write/update generated an uuid, we store uuid in the table.
	// It is useful in compaction, allowing us to know whether the kv
	// in the value log is up-to-date without accessing the index.
	// we always build deprecatedtable immediately after compaction,
	deprecatedtable struct {
		partition int
		// table   map[uint32]map[uuid.UUID]bool // we store deprecated uuid of keys,in memory
		table   map[uuid.UUID]bool
		size    uint32
		mu      sync.RWMutex
		options deprecatedtableOptions
		hasher	hash.Hash32
	}
	// The deprecatedtableOptions used to init dptable,
	// and we have set some default in DefaultOptions.
	// Here satisfy: lowerThreshold < upperThreshold <= capacity
	// When dptable size >= lowerThreshold,it notifies autoCompact try to compact,
	// and force compact when size arrive upperThreshold.
	deprecatedtableOptions struct {
		capacity       uint32
		lowerThreshold uint32
		upperThreshold uint32
	}

	// used to send message to autoCompact
	deprecatedtableState struct {
		partition      int
		thresholdState ThresholdState
	}
)


// Create a new deprecatedtable.
func newDeprecatedTable(partition int, options deprecatedtableOptions) *deprecatedtable {
	return &deprecatedtable{
		partition: partition,
		// table:     make(map[uint32]map[uuid.UUID]bool),  
		table:     make(map[uuid.UUID]bool),
		size:      0,
		options:   options,
		hasher:	   fnv.New32a(),
	}
}

func (dt *deprecatedtable) isFull() bool {
	return dt.size >= dt.options.capacity
}

func (dt *deprecatedtable) hashString(s string) uint32 {
	dt.hasher.Reset()
	dt.hasher.Write([]byte(s))
	return dt.hasher.Sum32() % KeyHash
}

// Add a uuid to the specified key.
func (dt *deprecatedtable) addEntry(key string, id uuid.UUID) error {
	// dt.mu.Lock()
	// defer dt.mu.Unlock()	
	if dt.isFull() {
		return ErrDtFull
	}
    // length := len(key)
    // if length > KeyHash {
    //     key = key[:KeyHash]
    // }
	// hash:= dt.hashString(key)
	// if dt.table[hash] == nil {
	// 	dt.table[hash] = make(map[uuid.UUID]bool)
	// }

	// dt.table[hash][id] = true
	dt.table[id] = true
	dt.size++
	return nil
}


func (dt *deprecatedtable) existEntry(key string, id uuid.UUID) bool {
	// dt.mu.RLock()
	// defer dt.mu.RUnlock()
    // length := len(key)
    // if length > KeyHash {
    //     key = key[:KeyHash]
    // }
	return dt.table[id]
	// hash:= dt.hashString(key)
	// if _, exists := dt.table[hash]; exists {
	// 	return dt.table[hash][id]
	// }
	// return false
}


func (dt *deprecatedtable) clean() {
	// dt.mu.Lock()
	// defer dt.mu.Unlock()
	// dt.table = make(map[uint32]map[uuid.UUID]bool)
	dt.table = make(map[uuid.UUID]bool)
	dt.size = 0
}
