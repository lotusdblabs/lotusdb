package domain

type ReadIndexComKvnode struct {
	Key   []byte
	Value []byte
}

type ReadIndexComponentConf interface {
	SetType(typ string)
	SetDbName(dbname string)
	SetFilePath(filePath string)

	GetType() (typ string)
	GetDbName() (dbname string)
	GetFilePath() (filePath string)
}

type ReadIndexTx interface{}

type ReadIndexComponent interface {
	Put(key []byte, value []byte) (err error)

	PutBatch(kv []ReadIndexComKvnode) (offset int, err error)

	Get(k []byte) (value []byte, err error)

	GetWithTx(tx ReadIndexTx, k []byte) (value []byte, err error)

	Delete(key []byte) error

	DeleteWithTx(tx ReadIndexTx, key []byte) error

	// Begin star a transaction.
	// You need to exec Begin before some methods that need to be put in a transaction.
	Begin(writeFlag bool) (ReadIndexTx, error)

	// Commit End a transaction.
	// If you have executed all read and write operations,
	// Call this method to submit.
	Commit(t ReadIndexTx) error

	// Rollback End a transaction.
	// If an error occurs during your read and write operations,
	// Call this method to rollback.
	Rollback(tx ReadIndexTx) error

	// First moves the cursor to the first item in the bucket and returns its key and value.
	// If the bucket is empty then a nil key and value are returned.
	First() (key, value []byte)

	// FirstWhitTx moves the cursor to the first item in the bucket and returns its key and value.
	// FirstWhitTx needs to be put into a transaction.
	// If the bucket is empty then a nil key and value are returned.
	FirstWhitTx(tx ReadIndexTx) (key, value []byte, err error)

	// Last moves the cursor to the last item in the bucket and returns its key and value.
	// If the bucket is empty then a nil key and value are returned.
	Last() (key, value []byte)

	// LastWhitTx moves the cursor to the last item in the bucket and returns its key and value.
	// LastWhitTx needs to be put into a transaction.
	// If the bucket is empty then a nil key and value are returned.
	LastWhitTx(tx ReadIndexTx) (key, value []byte, err error)

	// Seek moves the cursor to a given key and returns it.
	// If the key does not exist then the next key is used. If no keys follow, a nil key is returned.
	Seek(seek []byte) (key, value []byte)

	// SeekWithTx moves the cursor to a given key and returns it.
	// SeekWithTx needs to be put into a transaction.
	// If the key does not exist then the next key is used. If no keys follow, a nil key is returned.
	SeekWithTx(tx ReadIndexTx, seek []byte) (key, value []byte, err error)

	// Next moves the cursor to the next item in the bucket and returns its key and value.
	// If the cursor is at the end of the bucket then a nil key and value are returned.
	Next() (key, value []byte)

	// NextWithTx moves the cursor to the next item in the bucket and returns its key and value.
	// NextWithTx needs to be put into a transaction.
	// If the cursor is at the end of the bucket then a nil key and value are returned.
	NextWithTx(tx ReadIndexTx) (key, value []byte, err error)

	// Prev moves the cursor to the previous item in the bucket and returns its key and value.
	// If the cursor is at the beginning of the bucket then a nil key and value are returned.
	Prev() (key, value []byte)

	// PrevWithTx moves the cursor to the previous item in the bucket and returns its key and value.
	// PrevWithTx needs to be put into a transaction.
	// If the cursor is at the beginning of the bucket then a nil key and value are returned.
	PrevWithTx(tx ReadIndexTx) (key, value []byte, err error)

	// IterDelete removes the current key/value under the cursor from the bucket.
	// IterDelete fails if current key/value is a bucket or if the transaction is not writable.
	IterDelete() error

	// IterDeleteWithTx removes the current key/value under the cursor from the bucket.
	// IterDeleteWithTx needs to be put into a transaction.
	// IterDeleteWithTx fails if current key/value is a bucket or if the transaction is not writable.
	IterDeleteWithTx(tx ReadIndexTx) error

	// Update encapsulates a series of read and write operations.
	Update(fn func(tx ReadIndexTx) error) (err error)

	// View encapsulates a series of read operations.
	View(fn func(tx ReadIndexTx) error) (err error)

	Close() (err error)
}
