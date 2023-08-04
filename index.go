package lotusdb

const (
	// indexFileExt is the file extension for index files.
	indexFileExt = "INDEX.%d"
)

// Index is the interface for index implementations.
// An index is a key-value store that maps keys to chunk positions.
// The index is used to find the chunk position of a key.
//
// Currently, the only implementation is a BoltDB index.
// But you can implement your own index if you want.
type Index interface {
	// PutBatch put batch records to index
	PutBatch([]*KeyPosition) error

	// Get chunk position by key
	Get([]byte) (*KeyPosition, error)

	// DeleteBatch delete batch records from index
	DeleteBatch([][]byte) error

	// Sync sync index data to disk
	Sync() error

	// Close index
	Close() error
}

func openIndex(options indexOptions) (Index, error) {
	switch options.indexType {
	case indexBoltDB:
		return openIndexBoltDB(options)
	default:
		panic("unknown index type")
	}
}

type IndexType int8

const (
	// indexBoltDB is the BoltDB index type.
	indexBoltDB IndexType = iota
)

type indexOptions struct {
	indexType IndexType

	dirPath string // index directory path

	partitionNum int // index partition nums for sharding

	hashKeyFunction func([]byte) uint64 // hash function for sharding
}

// Iterator is an interface for iterating the index.
type indexIterator interface {
	// Rewind seek the first key in the index iterator.
	Rewind()

	// Seek move the iterator to the key which is
	// greater(less when reverse is true) than or equal to the specified key.
	Seek(key []byte)

	// Next moves the iterator to the next key.
	Next()

	// Key get the current key.
	Key() []byte

	// Value get the current value.
	Value() *KeyPosition

	// Valid returns whether the iterator is exhausted.
	Valid() bool

	// Close the iterator.
	Close()
}
