package lotusdb

import (
	"github.com/flowercorp/lotusdb/util"
	"os"
)

type LotusDB struct {
	lockMgr *LockMgr                 // global lock manager that guarantees consistency of read and write.
	cfs     map[string]*ColumnFamily // all column families.
	opts    Options
}

// Open a new LotusDB instance.
func Open(opt Options) (*LotusDB, error) {
	// add dir lock? todo
	if !util.PathExist(opt.DBPath) {
		if err := os.MkdirAll(opt.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &LotusDB{opts: opt, cfs: make(map[string]*ColumnFamily)}
	// load default column family.
	if opt.CfOpts.CfName == "" {
		opt.CfOpts.CfName = defaultColumnFamilyName
	}
	if _, err := db.OpenColumnFamily(opt.CfOpts); err != nil {
		return nil, err
	}
	return db, nil
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
