package lotusdb

import (
	"fmt"
	"sync"

	"github.com/bwmarrin/snowflake"
)

// Batch is a batch operations of the database.
// If readonly is true, you can only get data from the batch by Get method.
// An error will be returned if you try to use Put or Delete method.
//
// If readonly is false, you can use Put and Delete method to write data to the batch.
// The data will be written to the database when you call Commit method.
//
// Batch is not a transaction, it does not guarantee isolation.
// But it can guarantee atomicity, consistency and durability(if the Sync options is true).
//
// You must call Commit method to commit the batch, otherwise the DB will be locked.
type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchId       *snowflake.Node
}

// NewBatch creates a new Batch instance.
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
	}
	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LogRecord)
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func makeBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
	return b
}

func (b *Batch) withPendingWrites() *Batch {
	b.pendingWrites = make(map[string]*LogRecord)
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.committed = false
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put adds a key-value pair to the batch for writing.
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
	b.mu.Unlock()

	return nil
}

// Get retrieves the value associated with a given key from the batch.
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	// get from pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type == LogRecordDeleted {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
		b.mu.RUnlock()
	}

	// get from memtables
	tables := b.db.getMemTables()
	for _, table := range tables {
		deleted, value := table.get(key)
		if deleted {
			return nil, ErrKeyNotFound
		}
		if len(value) != 0 {
			return value, nil
		}
	}

	// get from index
	position, err := b.db.index.Get(key)
	if err != nil {
		return nil, err
	}
	if position == nil {
		return nil, ErrKeyNotFound
	}
	record, err := b.db.vlog.read(position)
	if err != nil {
		return nil, err
	}

	return record.value, nil
}

// Delete marks a key for deletion in the batch.
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:  key,
		Type: LogRecordDeleted,
	}
	b.mu.Unlock()

	return nil
}

// Exist checks if the key exists in the database.
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	// check if the key exists in pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			return record.Type != LogRecordDeleted, nil
		}
		b.mu.RUnlock()
	}

	// get from memtables
	tables := b.db.getMemTables()
	for _, table := range tables {
		deleted, value := table.get(key)
		if deleted {
			return false, nil
		}
		if len(value) != 0 {
			return true, nil
		}
	}

	// check if the key exists in index
	position, err := b.db.index.Get(key)
	if err != nil {
		return false, err
	}
	return position != nil, nil
}

// Commit commits the batch, if the batch is readonly or empty, it will return directly.
//
// It will iterate the pendingWrites and write the data to the database,
// then write a record to indicate the end of the batch to guarantee atomicity.
// Finally, it will write the index.
func (b *Batch) Commit(options *WriteOptions) error {
	// use the default options if options is nil
	if options == nil {
		options = &WriteOptions{Sync: false, DisableWal: false}
	}

	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// check if committed
	if b.committed {
		return ErrBatchCommitted
	}

	// wait for memtable space
	if err := b.db.waitMemetableSpace(); err != nil {
		return err
	}
	batchId := b.batchId.Generate()
	// call memtable put batch
	err := b.db.activeMem.putBatch(b.pendingWrites, batchId, options)
	if err != nil {
		return err
	}

	b.committed = true
	return nil
}
