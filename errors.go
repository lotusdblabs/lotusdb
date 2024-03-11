package lotusdb

import "errors"

var (
	ErrKeyIsEmpty                    = errors.New("the key is empty")
	ErrKeyNotFound                   = errors.New("key not found in database")
	ErrDatabaseIsUsing               = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch                 = errors.New("the batch is read only")
	ErrBatchCommitted                = errors.New("the batch is committed")
	ErrDBClosed                      = errors.New("the database is closed")
	ErrDBDirectoryISEmpty            = errors.New("the database directory path can not be empty")
	ErrWaitMemtableSpaceTimeOut      = errors.New("wait memtable space timeout, try again later")
	ErrDBIteratorUnsupportedTypeHASH = errors.New("hash index does not support iterator")
)
