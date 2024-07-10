package lotusdb

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

const ()

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
		table   map[string][]uuid.UUID // we store deprecated uuid of keys,in memory
		size	uint32
		mu		sync.Mutex
		options deprecatedtableOptions
	}
	// The deprecatedtableOptions used to init dptable, 
	// and we have set some default in DefaultOptions.
	// Here satisfy: lowerThreshold < upperThreshold <= capacity
	// When dptable size >= lowerThreshold,it notifies autoCompact try to compact,
	// and force compact when size arrive upperThreshold.
	deprecatedtableOptions struct {
		capacity uint32
		lowerThreshold uint32
		upperThreshold uint32
	}
)

// Create a new deprecatedtable.
func newDeprecatedTable(options deprecatedtableOptions) *deprecatedtable {
    return &deprecatedtable{
        table:   make(map[string][]uuid.UUID),
		size: 0,
        options: options,
    }
}

func (dt *deprecatedtable) isFull() bool {
	return dt.size >= dt.options.capacity
}

// Add a uuid to the specified key.
func (dt *deprecatedtable) addEntry(key string, id uuid.UUID) error {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	if dt.isFull() {
        return ErrDtFull
    }
	dt.size++
    dt.table[key] = append(dt.table[key], id)
	return nil
}

// Delete an entry by key and uuid.
func (dt *deprecatedtable) removeEntry(key string, id uuid.UUID) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
    if _, exists := dt.table[key]; !exists {
        return
    }
    // find and delete uuid
    for i, v := range dt.table[key] {
        if v == id {
			dt.size--
            dt.table[key] = append(dt.table[key][:i], dt.table[key][i+1:]...)
            return
        }
    }
}

// Find if an uuid exists based on key and uuid.
func (dt *deprecatedtable) existEntry(key string, id uuid.UUID) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()
    if _, exists := dt.table[key]; !exists {
        return false
    }
    for _, v := range dt.table[key] {
        if v == id {
            return true
        }
    }
    return false
}