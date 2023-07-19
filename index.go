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
