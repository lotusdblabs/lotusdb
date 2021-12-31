package index

import (
	"errors"
)

var (
	ErrColumnFamilyNameNil = errors.New("column family name is nil")
	ErrBucketNameNil       = errors.New("bucket name is nil")
	ErrDirPathNil          = errors.New("bptree dir path is nil")
	ErrBucketNotInit       = errors.New("bucket not init")
	ErrOptionsTypeNotMatch = errors.New("indexer options not match")
)

type IndexerKvnode struct {
	Key   []byte
	Value []byte
}

type IndexerType int8

const (
	BptreeBoltDB IndexerType = iota
)

type IndexerOptions interface {
	SetType(typ IndexerType)
	SetColumnFamilyName(cfName string)
	SetDirPath(dirPath string)

	GetType() IndexerType
	GetColumnFamilyName() string
	GetDirPath() string
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

func NewIndexer(opts IndexerOptions) (Indexer, error) {
	switch opts.GetType() {
	case BptreeBoltDB:
		boltOpts, ok := opts.(*BoltOptions)
		if !ok {
			return nil, ErrOptionsTypeNotMatch
		}
		return BptreeBolt(boltOpts)
	default:
		panic("unknown indexer type")
	}
}
