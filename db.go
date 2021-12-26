package lotusdb

type LotusDB struct {
	lockMgr *LockMgr                 // global lock manager that guarantees consistency of read and write.
	cfs     map[string]*ColumnFamily // all column families.
}

func Open(opt Options) (*LotusDB, error) {
	// load all column families.
	return nil, nil
}

// Close close database.
func (db *LotusDB) Close() error {
	return nil
}

// Put put to default column family.
func (db *LotusDB) Put(key, value []byte) error {
	return nil
}

// Get get from default column family.
func (db *LotusDB) Get(key []byte) error {
	return nil
}

// Delete delete from default column family.
func (db *LotusDB) Delete(key []byte) error {
	return nil
}
