package lotusdb

type DB struct {
	// Active memtable for writing.
	activeMem *memtable
	// Immutable memtables, waiting to be flushed to disk.
	immuMems []*memtable
	// vlog is the value log.
	vlog *valueLog
}

type Stat struct {
}

func Open() (*DB, error) {
	return nil, nil
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Sync() error {
	return nil
}

func (db *DB) Stat() *Stat {
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	return nil
}

func (db *DB) Get(key []byte, value []byte) ([]byte, error) {
	return nil, nil
}

func (db *DB) Delete(key []byte) error {
	return nil
}

func (db *DB) Exist(key []byte) (bool, error) {
	return false, nil
}
