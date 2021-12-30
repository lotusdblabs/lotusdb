package domain

type IndexComKvnode struct {
	Key   []byte
	Value []byte
}

type IndexComponentConf interface {
	SetType(typ string)
	SetDbName(dbname string)
	SetFilePath(filePath string)

	GetType() (typ string)
	GetDbName() (dbname string)
	GetFilePath() (filePath string)
}

// todo
type IndexTx interface{}

type IndexIter interface {
	// First moves the cursor to the first item in the bucket and returns its key and value.
	// If the bucket is empty then a nil key and value are returned.
	First() (key, value []byte)

	// Last moves the cursor to the last item in the bucket and returns its key and value.
	// If the bucket is empty then a nil key and value are returned.
	Last() (key, value []byte)

	// Seek moves the cursor to a given key and returns it.
	// If the key does not exist then the next key is used. If no keys follow, a nil key is returned.
	Seek(seek []byte) (key, value []byte)

	// Next moves the cursor to the next item in the bucket and returns its key and value.
	// If the cursor is at the end of the bucket then a nil key and value are returned.
	Next() (key, value []byte)

	// Prev moves the cursor to the previous item in the bucket and returns its key and value.
	// If the cursor is at the beginning of the bucket then a nil key and value are returned.
	Prev() (key, value []byte)

	// The close method must be called after the iterative behavior is over！
	Close() error
}

type IndexComponent interface {
	Put(key []byte, value []byte) (err error)

	PutBatch(kv []IndexComKvnode) (offset int, err error)

	Get(k []byte) (value []byte, err error)

	Delete(key []byte) error

	Close() (err error)

	Iter() (iter IndexIter, err error)

	// Update encapsulates a series of read and write operations.
	//Update(fn func(tx IndexTx) error) (err error)

	// View encapsulates a series of read operations.
	//View(fn func(tx IndexTx) error) (err error)
}
