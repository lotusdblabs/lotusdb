package index

import (
	"errors"
)

var (
	ErrDBNameNil     = errors.New("DB name is nil")
	ErrBucketNameNil = errors.New("bucket name is nil")
	ErrFilePathNil   = errors.New("file path is nil")
	ErrBucketNotInit = errors.New("bucket not init")
)

type IndexerKvnode struct {
	Key   []byte
	Value []byte
}

type IndexerOptions interface {
	SetType(typ string)
	SetDbName(dbname string)
	SetFilePath(filePath string)

	GetType() (typ string)
	GetDbName() (dbname string)
	GetFilePath() (filePath string)
}

type IndexerTx interface{}

type IndexerIter interface {
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

	// Close The close method must be called after the iterative behavior is over！
	Close() error
}

type Indexer interface {
	Put(key []byte, value []byte) (err error)

	PutBatch(kv []IndexerKvnode) (offset int, err error)

	Get(k []byte) (value []byte, err error)

	Delete(key []byte) error

	Close() (err error)

	Iter() (iter IndexerIter, err error)
}

func NewIndexer(opts IndexerOptions) (r Indexer, err error) {
	switch opts.GetType() {
	case IndexComponentTyp:
		bboltdbConfig, ok := opts.(*BboltdbConfig)
		if !ok {
			return nil, errors.New("options type error")
		}
		return NewBboltdb(bboltdbConfig)
	default:
		panic("unknown indexer type")
	}
}
