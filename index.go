package lotusdb

import "github.com/rosedblabs/wal"

const (
	indexFileExt        = ".INDEX.%d"
	defaultPartitionNum = 1
)

type Index interface {
	PutBatch([]*IndexRecord) error

	Get([]byte) (*wal.ChunkPosition, error)

	DeleteBatch([][]byte) error

	Sync() error

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
	indexBoltDB IndexType = iota
)

type indexOptions struct {
	indexType IndexType

	dirPath string

	partitionNum int

	hashKeyFunction func(string) uint32
}
