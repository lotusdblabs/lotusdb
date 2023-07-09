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

func Open(options Options) (*DB, error) {
	// check whether all options are valid

	// create the directory if not exist

	// acquire file lock

	// open all memtables

	// open index

	// open value log

	return nil, nil
}

func (db *DB) Close() error {
	// close all memtables

	// close index

	// close value log

	// release file lock

	return nil
}

func (db *DB) Sync() error {
	// sync all wal of memtables

	// sync index

	// sync value log
	return nil
}

func (db *DB) Stat() *Stat {
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	// call batch put
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// call batch get
	return nil, nil
}

func (db *DB) Delete(key []byte) error {
	// call batch delete
	return nil
}

func (db *DB) Exist(key []byte) (bool, error) {
	// call batch exist
	return false, nil
}
