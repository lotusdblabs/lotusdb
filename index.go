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
	case BTree:
		return openBTreeIndex(options)
	case Hash:
		return openHashIndex(options)
	default:
		panic("unknown index type")
	}
}

type IndexType int8

const (
	// BTree is the BoltDB index type.
	BTree IndexType = iota
	// Hash is the diskhash index type. see: https://github.com/rosedblabs/diskhash
	Hash
)

type indexOptions struct {
	indexType IndexType

	dirPath string // index directory path

	partitionNum int // index partition nums for sharding

	hashKeyFunction func([]byte) uint64 // hash function for sharding
}

func (io *indexOptions) getKeyPartition(key []byte) int {
	hashFn := io.hashKeyFunction
	return int(hashFn(key) % uint64(io.partitionNum))
}
